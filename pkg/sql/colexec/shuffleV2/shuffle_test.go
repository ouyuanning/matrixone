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

package shuffleV2

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 8192 // default rows
)

// add unit tests for cases
type shuffleTestCase struct {
	arg   *ShuffleV2
	types []types.Type
	proc  *process.Process
}

func makeTestCases(t *testing.T) []shuffleTestCase {
	return []shuffleTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int16.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     5,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     4,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     8,
				ShuffleColMin: 1,
				ShuffleColMax: 8888888,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint16.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     5,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint32.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     3,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint64.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     8,
				ShuffleColMin: 1,
				ShuffleColMax: 999999,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int16.ToType(),
			},
			arg: &ShuffleV2{
				ctr:               container{},
				ShuffleColIdx:     0,
				ShuffleType:       int32(plan.ShuffleType_Range),
				BucketNum:         5,
				ShuffleRangeInt64: []int64{1, 100, 10000, 100000},
				IsDebug:           true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &ShuffleV2{
				ctr:               container{},
				ShuffleColIdx:     0,
				ShuffleType:       int32(plan.ShuffleType_Range),
				BucketNum:         4,
				ShuffleRangeInt64: []int64{100, 10000, 100000},
				IsDebug:           true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &ShuffleV2{
				ctr:               container{},
				ShuffleColIdx:     0,
				ShuffleType:       int32(plan.ShuffleType_Range),
				BucketNum:         8,
				ShuffleRangeInt64: []int64{1, 100, 10000, 100000, 200000, 300000, 400000},
				IsDebug:           true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint16.ToType(),
			},
			arg: &ShuffleV2{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(plan.ShuffleType_Range),
				BucketNum:          5,
				ShuffleRangeUint64: []uint64{1, 100, 10000, 100000},
				IsDebug:            true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint32.ToType(),
			},
			arg: &ShuffleV2{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(plan.ShuffleType_Range),
				BucketNum:          3,
				ShuffleRangeUint64: []uint64{100, 10000},
				IsDebug:            true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint64.ToType(),
			},
			arg: &ShuffleV2{
				ctr:                container{},
				ShuffleColIdx:      0,
				ShuffleType:        int32(plan.ShuffleType_Range),
				BucketNum:          5,
				ShuffleRangeUint64: []uint64{1, 100, 10000, 100000},
				IsDebug:            true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     3,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint64.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     4,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     5,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint32.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     6,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int16.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     7,
				IsDebug:       true,
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_uint16.ToType(),
			},
			arg: &ShuffleV2{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     8,
				IsDebug:       true,
			},
		},
	}
}

func runShuffleCase(t *testing.T, tc shuffleTestCase, hasnull bool) {
	var result vm.CallResult
	var count int
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	resetChildren(tc.arg, getInputBats(tc, hasnull))
	for {
		result, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		count += result.Batch.RowCount()
	}
	tc.arg.ctr.shufflePool.DebugPrint()
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.arg.Reset(tc.proc, false, nil)
}

func TestShuffle(t *testing.T) {

	for _, tc := range makeTestCases(t) {
		runShuffleCase(t, tc, true)
		// second run
		runShuffleCase(t, tc, false)
		tc.proc.Free()
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestFree(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)

	}
}

func TestReset(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Reset(tc.proc, false, nil)
	}
}

func TestPrint(t *testing.T) {
	sp := NewShufflePool(4, 1)
	sp.DebugPrint()
}

func TestTypeName(t *testing.T) {
	s := NewArgument()
	logutil.Infof("%v", s.TypeName())
}

func getInputBats(tc shuffleTestCase, hasnull bool) []*batch.Batch {
	return []*batch.Batch{
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatch(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		newBatchSorted(tc.types, tc.proc, Rows, hasnull),
		batch.EmptyBatch,
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64, hasNull bool) *batch.Batch {
	if hasNull {
		return testutil.NewBatchWithNulls(ts, true, int(rows), proc.Mp())
	}
	return testutil.NewBatch(ts, true, int(rows), proc.Mp())
}

func newBatchSorted(ts []types.Type, proc *process.Process, rows int64, hasNull bool) *batch.Batch {
	if hasNull {
		return testutil.NewBatchWithNulls(ts, false, int(rows), proc.Mp())
	}
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *ShuffleV2, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
