// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type Intent interface {
	String() string
	Wait() <-chan struct{}
}

type EntryOption func(*CheckpointEntry)

func WithEndEntryOption(end types.TS) EntryOption {
	return func(e *CheckpointEntry) {
		e.end = end
	}
}

func WithStateEntryOption(state State) EntryOption {
	return func(e *CheckpointEntry) {
		e.state = state
	}
}

func WithCheckedEntryOption(policyChecked, flushedChecked bool) EntryOption {
	return func(e *CheckpointEntry) {
		e.policyChecked = policyChecked
		e.flushChecked = flushedChecked
	}
}

type CheckpointEntry struct {
	sync.RWMutex
	sid        string
	start, end types.TS
	state      State
	entryType  EntryType
	cnLocation objectio.Location
	tnLocation objectio.Location
	version    uint32

	ckpLSN      uint64
	truncateLSN uint64

	policyChecked bool
	flushChecked  bool

	// only for new entry logic procedure
	bornTime   time.Time
	refreshCnt uint32

	doneC chan struct{}
}

func NewCheckpointEntry(
	sid string, start, end types.TS, typ EntryType, opts ...EntryOption,
) *CheckpointEntry {
	e := &CheckpointEntry{
		sid:       sid,
		start:     start,
		end:       end,
		state:     ST_Pending,
		entryType: typ,
		version:   logtail.CheckpointCurrentVersion,
		bornTime:  time.Now(),
		doneC:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func InheritCheckpointEntry(
	from *CheckpointEntry,
	replaceOpts ...EntryOption,
) *CheckpointEntry {
	from.RLock()
	defer from.RUnlock()
	e := &CheckpointEntry{
		sid:           from.sid,
		start:         from.start,
		end:           from.end,
		state:         from.state,
		entryType:     from.entryType,
		version:       from.version,
		bornTime:      from.bornTime,
		refreshCnt:    from.refreshCnt,
		policyChecked: from.policyChecked,
		flushChecked:  from.flushChecked,
		doneC:         from.doneC,
	}
	for _, opt := range replaceOpts {
		opt(e)
	}
	return e
}

func (e *CheckpointEntry) MarshalJSON() ([]byte, error) {
	e.RLock()
	defer e.RUnlock()
	return json.Marshal(&struct {
		Start       string `json:"start"`
		End         string `json:"end"`
		State       State  `json:"state"`
		EntryType   string `json:"entry_type"`
		Location    string `json:"location"`
		CKPLSN      uint64 `json:"ckp_lsn"`
		TruncateLSN uint64 `json:"truncate_lsn"`
	}{
		Start:       e.start.ToString(),
		End:         e.end.ToString(),
		State:       e.state,
		EntryType:   e.entryType.String(),
		Location:    e.cnLocation.String(),
		CKPLSN:      e.ckpLSN,
		TruncateLSN: e.truncateLSN,
	})
}

// ================================================================
// ts comparison related
// ================================================================

// compare with other entry
// e.start >= o.end
func (e *CheckpointEntry) AllGE(o *CheckpointEntry) bool {
	return e.start.GE(&o.end)
}

// e.end <= ts
// it means that e is before ts
func (e *CheckpointEntry) LEByTS(ts *types.TS) bool {
	return e.end.LE(ts)
}

// false: e.start > to or e.end < from
// true: otherwise
func (e *CheckpointEntry) HasOverlap(from, to types.TS) bool {
	if e.start.GT(&to) || e.end.LT(&from) {
		return false
	}
	return true
}

// true: o.start == e.end + 1
func (e *CheckpointEntry) IsYoungNeighbor(o *CheckpointEntry) bool {
	next := e.end.Next()
	return o.start.Equal(&next)
}

//===============================================================
// execution related
//===============================================================

// it can be called multiple times
// it will block until Done() is called
func (e *CheckpointEntry) Wait() <-chan struct{} {
	return e.doneC
}

// it can only be called at most once
// it will unblock all Wait() calls
func (e *CheckpointEntry) Done() {
	close(e.doneC)
}

//===============================================================
// getter related
//===============================================================

func (e *CheckpointEntry) IsPolicyChecked() bool {
	e.RLock()
	defer e.RUnlock()
	return e.policyChecked
}

func (e *CheckpointEntry) IsFlushChecked() bool {
	e.RLock()
	defer e.RUnlock()
	return e.flushChecked
}

func (e *CheckpointEntry) AllChecked() bool {
	e.RLock()
	defer e.RUnlock()
	return e.policyChecked && e.flushChecked
}

func (e *CheckpointEntry) LSN() uint64 {
	e.RLock()
	defer e.RUnlock()
	return e.ckpLSN
}

func (e *CheckpointEntry) GetStart() types.TS { return e.start }
func (e *CheckpointEntry) GetEnd() types.TS   { return e.end }
func (e *CheckpointEntry) GetState() State {
	e.RLock()
	defer e.RUnlock()
	return e.state
}

func (e *CheckpointEntry) IsCommitted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}

func (e *CheckpointEntry) GetLocation() objectio.Location {
	e.RLock()
	defer e.RUnlock()
	return e.cnLocation
}

func (e *CheckpointEntry) GetTNLocation() objectio.Location {
	e.RLock()
	defer e.RUnlock()
	return e.tnLocation
}

func (e *CheckpointEntry) GetVersion() uint32 {
	e.RLock()
	defer e.RUnlock()
	return e.version
}

func (e *CheckpointEntry) GetTruncateLsn() uint64 {
	e.RLock()
	defer e.RUnlock()
	return e.truncateLSN
}

func (e *CheckpointEntry) IsIncremental() bool {
	return e.entryType == ET_Incremental
}

func (e *CheckpointEntry) IsGlobal() bool {
	return e.entryType == ET_Global
}

func (e *CheckpointEntry) IsBackup() bool {
	return e.entryType == ET_Backup
}

func (e *CheckpointEntry) IsCompact() bool {
	return e.entryType == ET_Compacted
}

func (e *CheckpointEntry) GetType() EntryType {
	return e.entryType
}

//===============================================================
// setter related
//===============================================================

func (e *CheckpointEntry) SetPolicyChecked() {
	e.Lock()
	defer e.Unlock()
	e.policyChecked = true
}

func (e *CheckpointEntry) SetFlushChecked() {
	e.Lock()
	defer e.Unlock()
	e.flushChecked = true
}

func (e *CheckpointEntry) SetVersion(version uint32) {
	e.Lock()
	defer e.Unlock()
	e.version = version
}

func (e *CheckpointEntry) SetLSN(ckpLSN, truncateLSN uint64) {
	e.Lock()
	defer e.Unlock()
	e.ckpLSN = ckpLSN
	e.truncateLSN = truncateLSN
}

func (e *CheckpointEntry) SetLocation(cn, tn objectio.Location) {
	e.Lock()
	defer e.Unlock()
	e.cnLocation = cn.Clone()
	e.tnLocation = tn.Clone()
}

//===============================================================
// born time related
//===============================================================

func (e *CheckpointEntry) DeferRetirement() {
	e.Lock()
	defer e.Unlock()
	e.refreshCnt++
}

func (e *CheckpointEntry) Age() time.Duration {
	e.RLock()
	defer e.RUnlock()
	return time.Since(e.bornTime)
}

func (e *CheckpointEntry) ResetAge() {
	e.Lock()
	defer e.Unlock()
	e.bornTime = time.Now()
	e.refreshCnt = 0
}

func (e *CheckpointEntry) TooOld() bool {
	e.RLock()
	defer e.RUnlock()
	return time.Since(e.bornTime) > time.Minute*3*time.Duration(e.refreshCnt+1)
}

//===============================================================
// state related
//===============================================================

func (e *CheckpointEntry) IsRunning() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Running
}

func (e *CheckpointEntry) IsPendding() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Pending
}

func (e *CheckpointEntry) IsFinished() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}

func (e *CheckpointEntry) SetState(state State) (ok bool) {
	e.Lock()
	defer e.Unlock()
	// entry is already finished
	if e.state == ST_Finished {
		return
	}
	// entry is already running
	if state == ST_Running && e.state == ST_Running {
		return
	}
	e.state = state
	ok = true
	return
}

//===============================================================
// string related
//===============================================================

func (e *CheckpointEntry) LSNString() string {
	e.RLock()
	defer e.RUnlock()
	return fmt.Sprintf("%d-%d", e.ckpLSN, e.truncateLSN)
}

func (e *CheckpointEntry) JsonString() string {
	json, err := e.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("bad entry: %s:%v", e.String(), err)
	}
	return string(json)
}

func (e *CheckpointEntry) String() string {
	if e == nil {
		return "nil"
	}
	t := e.GetType().String()
	state := e.GetState()
	return fmt.Sprintf(
		"CKP[%s][%v][%v:%v][%s](%s->%s)",
		t,
		state,
		e.IsPolicyChecked(),
		e.IsFlushChecked(),
		e.LSNString(),
		e.start.ToString(),
		e.end.ToString(),
	)
}

//===============================================================
// read related
//===============================================================

func (e *CheckpointEntry) GetTableRangesByID(
	ctx context.Context,
	tableID uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (ranges []ckputil.TableRange, err error) {
	reader := logtail.NewCKPReaderWithTableID_V2(e.version, e.cnLocation, tableID, mp, fs)
	if err = reader.ReadMeta(ctx); err != nil {
		return
	}
	return reader.GetTableRanges(ctx)
}

func (e *CheckpointEntry) GetTableRanges(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (ranges []ckputil.TableRange, err error) {
	reader := logtail.NewCKPReader(e.version, e.cnLocation, mp, fs)
	if err = reader.ReadMeta(ctx); err != nil {
		return
	}
	return reader.GetTableRanges(ctx)
}

// Only for test
// Only fill in columns ObjectID, CreateTS and DeleteTS
func (e *CheckpointEntry) GetTableByID(
	ctx context.Context, fs fileservice.FileService, tid uint64, mp *mpool.MPool,
) (data *batch.Batch, err error) {
	reader := logtail.NewCKPReaderWithTableID_V2(e.version, e.cnLocation, tid, mp, fs)
	if err = reader.ReadMeta(ctx); err != nil {
		return
	}
	totalLength := 0
	data = ckputil.NewObjectListBatch()
	if err = reader.ConsumeCheckpointWithTableID(
		ctx,
		func(
			ctx context.Context,
			obj objectio.ObjectEntry,
			isTombstone bool,
		) (err error) {
			totalLength++
			vector.AppendFixed[types.TS](
				data.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], obj.CreateTime, false, mp,
			)
			vector.AppendFixed[types.TS](
				data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], obj.CreateTime, false, mp,
			)
			vector.AppendBytes(
				data.Vecs[ckputil.TableObjectsAttr_ID_Idx], obj.ObjectStats[:], false, mp,
			)
			return
		},
	); err != nil {
		return
	}
	data.SetRowCount(totalLength)
	return
}

func (e *CheckpointEntry) GetCheckpointMetaInfo(
	ctx context.Context,
	id uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (res *logtail.ObjectInfoJson, err error) {
	reader := logtail.NewCKPReader(e.version, e.cnLocation, mp, fs)
	if err = reader.ReadMeta(ctx); err != nil {
		return
	}
	return logtail.GetCheckpointMetaInfo(ctx, id, reader)
}

func (e *CheckpointEntry) GetTableIDs(
	ctx context.Context,
	loc objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (result []uint64, err error) {
	reader := logtail.NewCKPReader(e.version, e.cnLocation, mp, fs)
	if err = reader.ReadMeta(ctx); err != nil {
		return
	}
	return logtail.GetTableIDsFromCheckpoint(ctx, reader)
}

func (e *CheckpointEntry) GetObjects(
	ctx context.Context,
	pinned map[string]bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	reader := logtail.NewCKPReader(e.version, e.cnLocation, mp, fs)
	if err = reader.ReadMeta(ctx); err != nil {
		return
	}
	return logtail.GetObjectsFromCKPMeta(ctx, reader, pinned)
}
