// Copyright 2023 Matrix Origin
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

package lockop

import (
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LockOp)

// FetchLockRowsFunc fetch lock rows from vector.
type FetchLockRowsFunc func(
	// primary data vector
	vec *vector.Vector,
	// holder to encode primary key to lock row
	parker *types.Packer,
	// primary key type
	tp types.Type,
	// global config: max lock rows bytes per lock
	max int,
	// is lock table lock
	lockTable bool,
	// used to filter rows
	filter RowsFilter,
	// used by filter rows func
	filterCols []int32) (bool, [][]byte, lock.Granularity)

// LockOptions lock operation options
type LockOptions struct {
	maxCountPerLock          int
	mode                     lock.LockMode
	sharding                 lock.Sharding
	group                    uint32
	lockTable                bool
	changeDef                bool
	parker                   *types.Packer
	fetchFunc                FetchLockRowsFunc
	filter                   RowsFilter
	filterCols               []int32
	hasNewVersionInRangeFunc hasNewVersionInRangeFunc
}

// LockOp lock op argument.
type LockOp struct {
	vm.OperatorBase

	logger  *log.MOLogger
	ctr     state
	engine  engine.Engine
	targets []lockTarget
}

func (lockOp *LockOp) GetOperatorBase() *vm.OperatorBase {
	return &lockOp.OperatorBase
}

func init() {
	reuse.CreatePool[LockOp](
		func() *LockOp {
			return &LockOp{}
		},
		func(a *LockOp) {
			*a = LockOp{}
		},
		reuse.DefaultOptions[LockOp]().
			WithEnableChecker(),
	)
}

func (lockOp LockOp) TypeName() string {
	return opName
}

func NewArgument() *LockOp {
	return reuse.Alloc[LockOp](nil)
}

func (lockOp *LockOp) Release() {
	if lockOp != nil {
		reuse.Free[LockOp](lockOp, nil)
	}
}

type lockTarget struct {
	tableID                      uint64
	objRef                       *plan.ObjectRef
	primaryColumnIndexInBatch    int32
	refreshTimestampIndexInBatch int32
	primaryColumnType            types.Type
	partitionColumnIndexInBatch  int32
	filter                       RowsFilter
	filterColIndexInBatch        int32
	lockTable                    bool
	changeDef                    bool
	mode                         lock.LockMode
	lockRows                     *plan.Expr
	lockTableAtTheEnd            bool
}

// RowsFilter used to filter row from primary vector. The row will not lock if filter return false.
type RowsFilter func(row int, filterCols []int32) bool

type hasNewVersionInRangeFunc func(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	bat *batch.Batch,
	idx int32,
	partitionIdx int32,
	from, to timestamp.Timestamp) (bool, error)

type state struct {
	parker               *types.Packer
	retryError           error
	defChanged           bool
	fetchers             []FetchLockRowsFunc
	relations            []engine.Relation
	hasNewVersionInRange hasNewVersionInRangeFunc
	lockCount            int64
}
