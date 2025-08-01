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

package logtailreplay

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"go.uber.org/zap"
)

// a partition corresponds to a dn
type Partition struct {
	//lock is used to protect pointer of PartitionState from concurrent mutation
	lock  chan struct{}
	state atomic.Pointer[PartitionState]

	// assuming checkpoints will be consumed once
	checkpointConsumed atomic.Bool

	TableInfo   TableInfo
	TableInfoOK bool
}

type TableInfo struct {
	ID            uint64
	Name          string
	PrimarySeqnum int
}

func NewPartition(
	service string,
	id uint64,
) *Partition {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	ret := &Partition{
		lock: lock,
	}
	ret.state.Store(NewPartitionState(service, false, id))
	return ret
}

func (p *Partition) Snapshot() *PartitionState {
	return p.state.Load()
}

func (*Partition) CheckPoint(ctx context.Context, ts timestamp.Timestamp) error {
	panic("unimplemented")
}

func (p *Partition) MutateState() (*PartitionState, func()) {
	curState := p.state.Load()
	state := curState.Copy()
	return state, func() {
		if !p.state.CompareAndSwap(curState, state) {
			panic("concurrent mutation")
		}
	}
}

func (p *Partition) Lock(ctx context.Context) error {
	select {
	case <-p.lock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Partition) Unlock() {
	p.lock <- struct{}{}
}

func (p *Partition) ConsumeSnapCkps(
	_ context.Context,
	ckps []*checkpoint.CheckpointEntry,
	fn func(
		ckp *checkpoint.CheckpointEntry,
		state *PartitionState,
	) error,
) (
	err error,
) {
	if len(ckps) == 0 {
		return nil
	}
	//Notice that checkpoints must contain only one or zero global checkpoint
	//followed by zero or multi continuous incremental checkpoints.
	state := p.state.Load()
	start := types.MaxTs()
	end := types.TS{}
	for i, ckp := range ckps {
		if err = fn(ckp, state); err != nil {
			return
		}
		if ckp.GetType() == checkpoint.ET_Global ||
			(ckp.GetType() == checkpoint.ET_Compacted && i == 0) {
			ckpStart := ckp.GetStart()
			if ckpStart.IsEmpty() && ckp.GetType() == checkpoint.ET_Global {
				start = ckp.GetEnd()
			} else {
				start = ckp.GetStart()
				end = ckp.GetEnd()
			}
		}
		if ckp.GetType() == checkpoint.ET_Incremental ||
			(ckp.GetType() == checkpoint.ET_Compacted && i > 0) {
			ckpstart := ckp.GetStart()
			if ckpstart.LT(&start) {
				start = ckpstart
			}
			ckpend := ckp.GetEnd()
			if ckpend.GT(&end) {
				end = ckpend
			}
		}
	}
	if end.IsEmpty() {
		//only one global checkpoint.
		end = start
	}
	state.UpdateDuration(start, end)
	if !state.IsValid() {
		return moerr.NewInternalErrorNoCtx("invalid checkpoints duration")
	}
	return nil
}

// ConsumeCheckpoints load and consumes all checkpoints in the partition, if consumed, it will return immediately.
func (p *Partition) ConsumeCheckpoints(
	ctx context.Context,
	fn func(
		checkpoint string,
		state *PartitionState,
	) error,
) (
	err error,
) {

	if p.checkpointConsumed.Load() {
		return nil
	}

	curState := p.state.Load()
	if len(curState.checkpoints) == 0 {
		return nil
	}

	lockErr := p.Lock(ctx)
	if lockErr != nil {
		return lockErr
	}
	defer p.Unlock()

	curState = p.state.Load()
	if len(curState.checkpoints) == 0 {
		return nil
	}

	state := curState.Copy()

	//consume checkpoints.
	if err := state.consumeCheckpoints(fn); err != nil {
		return err
	}

	if !p.state.CompareAndSwap(curState, state) {
		panic("concurrent mutation")
	}

	p.checkpointConsumed.Store(true)

	return
}

func (p *Partition) Truncate(ctx context.Context, ids [2]uint64, ts types.TS) error {
	err := p.Lock(ctx)
	if err != nil {
		return err
	}
	defer p.Unlock()
	curState := p.state.Load()

	state := curState.Copy()

	inst := time.Now()
	if updated := state.truncate(ids, ts); !updated {
		return nil
	}

	if !p.state.CompareAndSwap(curState, state) {
		panic("concurrent mutation")
	}

	logutil.Info(
		"PS-Truncate",
		zap.String("name", p.TableInfo.Name),
		zap.Uint64("id", p.TableInfo.ID),
		zap.String("prev-state", fmt.Sprintf("%p", curState)),
		zap.String("new-state", fmt.Sprintf("%p", state)),
		zap.Duration("duration", time.Since(inst)),
		zap.String("new-start", state.start.ToString()),
	)

	return nil
}
