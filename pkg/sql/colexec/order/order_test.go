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

package order

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type orderTestCase struct {
	arg   *Order
	types []types.Type
	proc  *process.Process
}

func makeTestCases(t *testing.T) []orderTestCase {
	return []orderTestCase{
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}, {Expr: newExpression(1), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 2}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestOrder(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []orderTestCase{
			newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
			newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)

			bats := []*batch.Batch{
				newBatch(tc.types, tc.proc, BenchmarkRows),
				newBatch(tc.types, tc.proc, BenchmarkRows),
				batch.EmptyBatch,
			}
			resetChildren(tc.arg, bats)
			_, _ = vm.Exec(tc.arg, tc.proc)
			tc.arg.Free(tc.proc, false, nil)
			tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		}
	}
}

func newTestCase(t testing.TB, ts []types.Type, fs []*plan.OrderBySpec) orderTestCase {
	return orderTestCase{
		types: ts,
		proc:  testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
		arg: &Order{
			OrderBySpec: fs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Order, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
