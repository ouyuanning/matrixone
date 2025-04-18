// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

const DefaultLoadParallism = 20

func (tbl *txnTable) CollectChanges(
	ctx context.Context,
	from, to types.TS,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	if from.IsEmpty() {
		return NewCheckpointChangesHandle(ctx, tbl, to, mp)
	}
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	return logtailreplay.NewChangesHandler(
		ctx,
		state,
		from, to,
		objectio.BlockMaxRows,
		tbl.primarySeqnum,
		mp,
		tbl.getTxn().engine.fs,
	)
}

type CheckpointChangesHandle struct {
	end    types.TS
	table  *txnTable
	fs     fileservice.FileService
	reader engine.Reader
	attrs  []string
	isEnd  bool

	sid         string
	blockList   objectio.BlockInfoSlice
	prefetchIdx int
	readIdx     int

	duration      time.Duration
	dataLength    int
	lastPrintTime time.Time
}

func NewCheckpointChangesHandle(
	ctx context.Context,
	table *txnTable,
	end types.TS,
	mp *mpool.MPool,
) (*CheckpointChangesHandle, error) {
	handle := &CheckpointChangesHandle{
		end:   end,
		table: table,
		fs:    table.getTxn().engine.fs,
		sid:   table.proc.Load().GetService(),
	}
	err := handle.initReader(ctx)
	return handle, err
}
func (h *CheckpointChangesHandle) prefetch() {
	blkCount := h.blockList.Len()
	for i := 0; i < DefaultLoadParallism; i++ {
		if h.prefetchIdx >= blkCount {
			return
		}
		blk := h.blockList.Get(h.prefetchIdx)
		err := ioutil.Prefetch(h.sid, h.fs, blk.MetaLoc[:])
		if err != nil {
			logutil.Warnf("ChangesHandle: prefetch failed: %v", err)
		}
		h.prefetchIdx++
	}
}
func (h *CheckpointChangesHandle) Next(
	ctx context.Context, mp *mpool.MPool,
) (
	data *batch.Batch,
	tombstone *batch.Batch,
	hint engine.ChangesHandle_Hint,
	err error,
) {
	if time.Since(h.lastPrintTime) > time.Minute {
		h.lastPrintTime = time.Now()
		if h.dataLength != 0 {
			logutil.Infof("ChangesHandle-Slow, data length %d, duration %v", h.dataLength, h.duration)
		}
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	hint = engine.ChangesHandle_Snapshot
	if h.isEnd {
		return nil, nil, hint, nil
	}
	tblDef := h.table.GetTableDef(ctx)
	if h.readIdx >= h.prefetchIdx {
		h.prefetch()
	}

	t0 := time.Now()
	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(len(tblDef.Cols))
		for i, col := range tblDef.Cols {
			bat.Attrs = append(bat.Attrs, col.Name)
			typ := plan2.ExprType2Type(&col.Typ)
			bat.Vecs[i] = vector.NewVec(typ)
		}
		return bat
	}
	data = buildBatch()
	h.isEnd, err = h.reader.Read(
		ctx,
		h.attrs,
		nil,
		mp,
		data,
	)
	h.readIdx++
	if h.isEnd {
		return nil, nil, hint, nil
	}
	if err != nil {
		return
	}

	committs, err := vector.NewConstFixed(types.T_TS.ToType(), h.end, data.Vecs[0].Length(), mp)
	if err != nil {
		data.Clean(mp)
		return
	}
	rowidVec := data.Vecs[len(data.Vecs)-1]
	rowidVec.Free(mp)
	data.Vecs[len(data.Vecs)-1] = committs
	data.Attrs[len(data.Attrs)-1] = objectio.DefaultCommitTS_Attr
	h.duration += time.Since(t0)
	h.dataLength += data.Vecs[0].Length()
	return
}
func (h *CheckpointChangesHandle) Close() error {
	h.reader.Close()
	return nil
}
func (h *CheckpointChangesHandle) initReader(ctx context.Context) (err error) {
	tblDef := h.table.GetTableDef(ctx)
	h.attrs = make([]string, 0)
	for _, col := range tblDef.Cols {
		h.attrs = append(h.attrs, col.Name)
	}

	var part *logtailreplay.PartitionState
	if part, err = h.table.getPartitionState(ctx); err != nil {
		return
	}

	var blockList objectio.BlockInfoSlice
	if _, err = readutil.TryFastFilterBlocks(
		ctx,
		h.end.ToTimestamp(),
		tblDef,
		engine.DefaultRangesParam,
		part,
		nil,
		nil,
		&blockList,
		h.table.PrefetchAllMeta,
		h.fs,
	); err != nil {
		return
	}
	relData := readutil.NewBlockListRelationData(
		1,
		readutil.WithPartitionState(part))
	h.blockList = blockList
	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(blockList.Get(i))
	}

	readers, err := h.table.BuildReaders(
		ctx,
		h.table.proc.Load(),
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckCommittedOnly,
		engine.FilterHint{},
	)
	if err != nil {
		return
	}
	h.reader = readers[0]

	return
}
