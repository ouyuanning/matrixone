// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fromValueListToVector(
	mp *mpool.MPool,
	typ types.Type, values any, isNull []bool) *vector.Vector {
	var err error

	v := vector.NewVec(typ)

	if typ.IsVarlen() {
		sts := values.([]string)

		if len(isNull) > 0 {
			for i, value := range sts {
				if err = vector.AppendBytes(v, []byte(value), isNull[i], mp); err != nil {
					break
				}
			}
		} else {
			for _, value := range sts {
				if err = vector.AppendBytes(v, []byte(value), false, mp); err != nil {
					break
				}
			}
		}

	} else {
		switch typ.Oid {
		case types.T_int64:
			err = vector.AppendFixedList[int64](v, values.([]int64), isNull, mp)

		case types.T_bool:
			err = vector.AppendFixedList[bool](v, values.([]bool), isNull, mp)

		case types.T_decimal128:
			err = vector.AppendFixedList[types.Decimal128](v, values.([]types.Decimal128), isNull, mp)

		default:
			panic(fmt.Sprintf("test util do not support the type %s now", typ))
		}
	}

	if err != nil {
		panic(err)
	}
	return v
}

type avgDemoCtx struct {
	count int64
}

func (d *avgDemoCtx) Marshal() []byte    { return types.EncodeInt64(&d.count) }
func (d *avgDemoCtx) Unmarshal(b []byte) { d.count = types.DecodeInt64(b) }
func (d *avgDemoCtx) Size() int64        { return 8 } // size of count
var _ AggGroupExecContext = &avgDemoCtx{}

func fromIdxListToNullList(start, end int, idxList []int) []bool {
	if len(idxList) == 0 {
		return nil
	}

	bs := make([]bool, end-start+1)
	for _, idx := range idxList {
		if realIndex := idx - start; realIndex >= 0 && idx <= end {
			bs[realIndex] = true
		}
	}
	return bs
}

func doAggTest[input, output types.FixedSizeTExceptStrType | string](
	t *testing.T,
	agg AggFuncExec,
	mp *mpool.MPool, paramType types.Type,
	group1 []input, nullList1 []int, result1 output, isResult1Null bool,
	group2 []input, nullList2 []int, result2 output, isResult2Null bool,
) {

	// tool methods.
	cutGroup := func(
		typ types.Type,
		group []input, nullList []int,
	) (v1, v2, v3 *vector.Vector) {
		// we cut the group input data as 3 part for test the following methods.
		// 1. fill.
		// 2. bulk fill.
		// 3. batch fill.
		switch len(group) {
		case 0:
			v1, v2, v3 = nil, nil, nil
		case 1:
			v1 = fromValueListToVector(mp, typ, group, fromIdxListToNullList(0, 0, nullList))
			v2 = nil
			v3 = nil
		case 2:
			v1 = fromValueListToVector(mp, typ, group[:1], fromIdxListToNullList(0, 0, nullList))
			v2 = fromValueListToVector(mp, typ, group[1:], fromIdxListToNullList(1, 1, nullList))
			v3 = nil
		default:
			gap := len(group) / 3
			v1 = fromValueListToVector(mp, typ, group[:gap], fromIdxListToNullList(0, gap-1, nullList))
			v2 = fromValueListToVector(mp, typ, group[gap:2*gap], fromIdxListToNullList(gap, 2*gap-1, nullList))
			v3 = fromValueListToVector(mp, typ, group[2*gap:], fromIdxListToNullList(2*gap, len(group)-1, nullList))
		}

		return v1, v2, v3
	}

	fillToGroup := func(
		idx int, v1, v2, v3 *vector.Vector) {

		// 1. fill
		if v1 != nil {
			vs := []*vector.Vector{v1}
			for i, j := 0, v1.Length(); i < j; i++ {
				require.NoError(t, agg.Fill(idx, i, vs))
			}
		}

		// 2. bulk fill
		if v2 != nil {
			vs := []*vector.Vector{v2}
			require.NoError(t, agg.BulkFill(idx, vs))
		}

		// 3. batch fill
		if v3 != nil {
			gs := make([]uint64, v3.Length())
			v := uint64(idx + 1)
			for i := range gs {
				gs[i] = v
			}
			vs := []*vector.Vector{v3}
			require.NoError(t, agg.BatchFill(0, gs, vs))
		}
	}

	checkResult := func(
		expectedNull bool, expectedResult output, resultV *vector.Vector, row uint64) {

		if expectedNull {
			require.True(t, resultV.IsNull(row))
		} else {
			if resultV.GetType().IsVarlen() {
				require.Equal(t, expectedResult, string(resultV.GetBytesAt(int(row))))
			} else {
				require.Equal(t, expectedResult, vector.GetFixedAtNoTypeCheck[output](resultV, int(row)))
			}
		}
	}

	// Real Logic start from here.
	// 1. fill group1 first.
	// 2. fill group2 second.
	// 3. add merge action (not implement now).
	// 4. marshal and unmarshal (not implement now).
	// 5. flush the result and do result check.
	require.NoError(t, agg.GroupGrow(1))
	q1, q2, q3 := cutGroup(paramType, group1, nullList1)
	fillToGroup(0, q1, q2, q3)

	require.NoError(t, agg.GroupGrow(1))
	p1, p2, p3 := cutGroup(paramType, group2, nullList2)
	fillToGroup(1, p1, p2, p3)

	rs, err := agg.Flush()
	require.NoError(t, err)

	require.Equal(t, 1, len(rs), "doAggTest() only support test with small amount of data.")

	checkResult(isResult1Null, result1, rs[0], 0)
	checkResult(isResult2Null, result2, rs[0], 1)
}

type hackManager struct {
	mp *mpool.MPool
}

func (h hackManager) Mp() *mpool.MPool {
	return h.mp
}

func hackAggMemoryManager() hackManager {
	return hackManager{mp: mpool.MustNewZeroNoFixed()}
}

func TestCount(t *testing.T) {
	m := hackAggMemoryManager()
	info := singleAggInfo{
		aggID:    AggIdOfCountColumn,
		distinct: false,
		retType:  types.T_int64.ToType(),
	}
	a := newCountColumnExecExec(m, info)

	doAggTest[int64, int64](
		t, a,
		m.Mp(), types.T_int64.ToType(),
		[]int64{1, 2, 3}, []int{0}, 2, false,
		[]int64{1, 2, 3}, nil, 3, false)

	a.Free()
}

func TestMedian(t *testing.T) {
	m := hackAggMemoryManager()
	info := singleAggInfo{
		aggID:     1,
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   types.T_float64.ToType(),
		emptyNull: true,
	}
	a := newMedianColumnNumericExec[int64](m, info)

	a.PreAllocateGroups(1)

	doAggTest[int64, float64](
		t, a,
		m.Mp(), types.T_int64.ToType(),
		[]int64{1, 2, 3}, nil, float64(2), false,
		[]int64{1, 2, 3}, nil, float64(2), false)

	_, err := MarshalAggFuncExec(a)
	assert.NoError(t, err)

	a.Merge(a, 0, 1)

	info2 := singleAggInfo{
		aggID:     1,
		distinct:  true,
		argType:   types.T_int64.ToType(),
		retType:   types.T_float64.ToType(),
		emptyNull: true,
	}
	b := newMedianColumnNumericExec[int64](m, info2)

	doAggTest[int64, float64](
		t, b,
		m.Mp(), types.T_int64.ToType(),
		[]int64{1, 2, 3}, nil, float64(2), false,
		[]int64{1, 2, 3}, nil, float64(2), false)
}

func TestBytesToBytesFrameWork(t *testing.T) {
	m := hackAggMemoryManager()
	info := singleAggInfo{
		distinct:  false,
		argType:   types.T_varchar.ToType(),
		retType:   types.T_varchar.ToType(),
		emptyNull: true,
	}
	implement := aggImplementation{
		ret: func(i []types.Type) types.Type {
			return types.T_varchar.ToType()
		},
		ctx: aggContextImplementation{
			hasCommonContext: false,
			hasGroupContext:  false,
		},
		logic: aggLogicImplementation{
			init: InitBytesResultOfAgg(
				func(resultType types.Type, parameters ...types.Type) []byte {
					return []byte("")
				}),

			fill: bytesBytesFill(
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, value []byte, aggIsEmpty bool, resultGetter AggBytesGetter, resultSetter AggBytesSetter) error {
					if len(resultGetter()) < len(value) {
						return resultSetter(value)
					}
					return nil
				}),

			fills: bytesBytesFills(
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, value []byte, count int, aggIsEmpty bool, resultGetter AggBytesGetter, resultSetter AggBytesSetter) error {
					if len(resultGetter()) < len(value) {
						return resultSetter(value)
					}
					return nil
				}),

			merge: bytesBytesMerge(
				func(ctx1, ctx2 AggGroupExecContext, commonContext AggCommonExecContext, aggIsEmpty1, aggIsEmpty2 bool, resultGetter1, resultGetter2 AggBytesGetter, resultSetter AggBytesSetter) error {
					panic("not implement now.")
				}),

			flush: bytesBytesFlush(
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, resultGetter AggBytesGetter, resultSetter AggBytesSetter) error {
					return nil
				}),
		},
	}

	a := newAggregatorFromBytesToBytes(
		m, info, implement)

	doAggTest[string, string](
		t, a,
		m.Mp(), types.T_varchar.ToType(),
		[]string{"a", "bb", "c", "ddd"}, []int{3}, "bb", false,
		[]string{"a", "bb", "c", "ddd"}, nil, "ddd", false)
}

func TestFixedToFixedFrameWork(t *testing.T) {
	m := hackAggMemoryManager()
	info := singleAggInfo{
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: true,
	}

	// a demo agg to count the odd number.
	implement := aggImplementation{
		ret: func(i []types.Type) types.Type {
			return types.T_int64.ToType()
		},
		ctx: aggContextImplementation{
			hasCommonContext: false,
			hasGroupContext:  false,
		},
		logic: aggLogicImplementation{
			init: InitFixedResultOfAgg[int64](
				func(resultType types.Type, parameters ...types.Type) int64 {
					return 0
				}),

			fill: fixedFixedFill[int64, int64](
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, value int64, aggIsEmpty bool, resultGetter AggGetter[int64], resultSetter AggSetter[int64]) error {
					if value%2 == 1 {
						resultSetter(resultGetter() + 1)
					}
					return nil
				}),

			fills: fixedFixedFills[int64, int64](
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, value int64, count int, aggIsEmpty bool, resultGetter AggGetter[int64], resultSetter AggSetter[int64]) error {
					if value%2 == 1 {
						resultSetter(resultGetter() + int64(count))
					}
					return nil
				}),

			merge: fixedFixedMerge[int64, int64](
				func(ctx1, ctx2 AggGroupExecContext, commonContext AggCommonExecContext, aggIsEmpty1, aggIsEmpty2 bool, resultGetter1, resultGetter2 AggGetter[int64], resultSetter AggSetter[int64]) error {
					resultSetter(resultGetter1() + resultGetter2())
					return nil
				}),

			flush: fixedFixedFlush[int64, int64](
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, resultGetter AggGetter[int64], resultSetter AggSetter[int64]) error {
					return nil
				}),
		},
	}

	a := newSingleAggFuncExec1NewVersion(
		m, info, implement)

	doAggTest[int64, int64](
		t, a,
		m.Mp(), types.T_int64.ToType(),
		[]int64{1, 2, 3, 4, 5}, nil, int64(3), false,
		[]int64{2, 3, 4, 5, 6}, nil, int64(2), false)
}

func TestFixedToFixedFrameWork_withExecContext(t *testing.T) {
	m := hackAggMemoryManager()
	info := singleAggInfo{
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: true,
	}

	implement := aggImplementation{
		ret: func(i []types.Type) types.Type {
			return types.T_int64.ToType()
		},
		ctx: aggContextImplementation{
			hasCommonContext: false,
			hasGroupContext:  true,
			generateGroupContext: func(resultType types.Type, parameters ...types.Type) AggGroupExecContext {
				return &avgDemoCtx{count: 0}
			},
		},
		logic: aggLogicImplementation{
			init: InitFixedResultOfAgg[int64](
				func(resultType types.Type, parameters ...types.Type) int64 {
					return 0
				}),

			fill: fixedFixedFill[int64, int64](
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, value int64, aggIsEmpty bool, resultGetter AggGetter[int64], resultSetter AggSetter[int64]) error {
					execContext.(*avgDemoCtx).count++
					resultSetter(resultGetter() + value)
					return nil
				}),

			fills: fixedFixedFills[int64, int64](
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, value int64, count int, aggIsEmpty bool, resultGetter AggGetter[int64], resultSetter AggSetter[int64]) error {
					execContext.(*avgDemoCtx).count += int64(count)
					resultSetter(resultGetter() + value*int64(count))
					return nil
				}),

			merge: fixedFixedMerge[int64, int64](
				func(ctx1, ctx2 AggGroupExecContext, commonContext AggCommonExecContext, aggIsEmpty1, aggIsEmpty2 bool, resultGetter1, resultGetter2 AggGetter[int64], resultSetter AggSetter[int64]) error {
					ctx1.(*avgDemoCtx).count += ctx2.(*avgDemoCtx).count
					resultSetter(resultGetter1() + resultGetter2())
					return nil
				}),

			flush: fixedFixedFlush[int64, int64](
				func(execContext AggGroupExecContext, commonContext AggCommonExecContext, resultGetter AggGetter[int64], resultSetter AggSetter[int64]) error {
					count := execContext.(*avgDemoCtx).count
					if count > 0 {
						resultSetter(resultGetter() / int64(count))
					}
					return nil
				}),
		},
	}

	a := newSingleAggFuncExec1NewVersion(
		m, info, implement)

	doAggTest[int64, int64](
		t, a,
		m.Mp(), types.T_int64.ToType(),
		[]int64{1, 2, 3, 4, 5}, nil, int64(15/5), false,
		[]int64{2, 3, 4, 5, 6}, []int{3}, int64(15/4), false)
}

func TestMakeInitialAggListFromList(t *testing.T) {
	mp := mpool.MustNewZero()

	RegisterGroupConcatAgg(123, ",")
	mg := NewSimpleAggMemoryManager(mp)
	agg0, err := MakeAgg(mg, 123, true, []types.Type{types.T_varchar.ToType()}...)
	require.Nil(t, err)
	res, err := MakeInitialAggListFromList(mg, []AggFuncExec{agg0})
	require.Nil(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, int64(123), res[0].AggID())
	require.Equal(t, true, res[0].IsDistinct())
}

func TestAggExecSize(t *testing.T) {
	m := hackAggMemoryManager()
	defer func() {
		// ensure all memory is released
		require.Equal(t, int64(0), m.Mp().CurrNB())
	}()

	testCases := []struct {
		name       string
		factory    func(mg AggMemoryManager) (AggFuncExec, error)
		groupCount int
		// fillFunc fills data into the aggregator and returns whether the size is expected to increase.
		fillFunc func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) (sizeShouldIncrease bool)
	}{
		{
			name: "count_star",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				return makeCount(mg, true, AggIdOfCountStar, false, types.T_int64.ToType()), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return false // Size should not increase as it only updates a counter.
			},
		},
		{
			name: "count_column",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				return makeCount(mg, false, AggIdOfCountColumn, false, types.T_int64.ToType()), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return false // Size should not increase as it only updates a counter.
			},
		},
		{
			name: "count_column_distinct",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				return makeCount(mg, false, AggIdOfCountColumn, true, types.T_int64.ToType()), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return true // Size should increase due to distinct hash map.
			},
		},
		{
			name: "median",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				info := singleAggInfo{
					aggID:     AggIdOfMedian,
					distinct:  false,
					argType:   types.T_int64.ToType(),
					retType:   MedianReturnType([]types.Type{types.T_int64.ToType()}),
					emptyNull: true,
				}
				return newMedianColumnNumericExec[int64](mg, info), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return true // Size should increase as it buffers all values.
			},
		},
		{
			name: "group_concat",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				return makeGroupConcat(mg, AggIdOfGroupConcat, false, []types.Type{types.T_varchar.ToType()}, getCroupConcatRet(types.T_varchar.ToType()), ","), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_varchar.ToType(), []string{"a", "b", "c"}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return false // Size should increase as the result string grows.
			},
		},
		{
			name: "approx_count",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				return makeApproxCount(mg, AggIdOfApproxCount, types.T_int64.ToType()), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return true // Size should increase due to HLL sketch.
			},
		},
		{
			name: "window_rank",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				return makeWindowExec(mg, WinIdOfRank, false)
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					for j := 0; j < v.Length(); j++ {
						require.NoError(t, agg.Fill(i, j, []*vector.Vector{v}))
					}
				}
				return true // Size should increase as it buffers partition data.
			},
		},
		{
			name: "fixed_to_fixed_with_context",
			factory: func(mg AggMemoryManager) (AggFuncExec, error) {
				info := singleAggInfo{
					distinct:  false,
					argType:   types.T_int64.ToType(),
					retType:   types.T_int64.ToType(),
					emptyNull: true,
				}
				implement := aggImplementation{
					ret: func(i []types.Type) types.Type { return types.T_int64.ToType() },
					ctx: aggContextImplementation{
						hasGroupContext: true,
						generateGroupContext: func(resultType types.Type, parameters ...types.Type) AggGroupExecContext {
							return &avgDemoCtx{count: 0}
						},
					},
					logic: aggLogicImplementation{
						init: InitFixedResultOfAgg[int64](func(r types.Type, p ...types.Type) int64 { return 0 }),
						fill: fixedFixedFill[int64, int64](func(g AggGroupExecContext, c AggCommonExecContext, v int64, e bool, r AggGetter[int64], s AggSetter[int64]) error {
							return nil
						}),
						fills: fixedFixedFills[int64, int64](func(g AggGroupExecContext, c AggCommonExecContext, v int64, count int, e bool, r AggGetter[int64], s AggSetter[int64]) error {
							return nil
						}),
						merge: fixedFixedMerge[int64, int64](func(g1, g2 AggGroupExecContext, c AggCommonExecContext, e1, e2 bool, r1, r2 AggGetter[int64], s AggSetter[int64]) error {
							return nil
						}),
					},
				}
				return newSingleAggFuncExec1NewVersion(mg, info, implement), nil
			},
			groupCount: 10,
			fillFunc: func(t *testing.T, agg AggFuncExec, mp *mpool.MPool, groupCount int) bool {
				v := fromValueListToVector(mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5}, nil)
				defer v.Free(mp)
				for i := 0; i < groupCount; i++ {
					require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
				}
				return false // Size should not increase.
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := m.Mp().CurrNB()

			agg, err := tc.factory(m)
			require.NoError(t, err)

			initialSize := agg.Size()
			require.GreaterOrEqual(t, initialSize, int64(0))

			err = agg.GroupGrow(tc.groupCount)
			require.NoError(t, err)
			grownSize := agg.Size()
			require.Greater(t, grownSize, initialSize)

			if tc.fillFunc != nil {
				sizeShouldIncrease := tc.fillFunc(t, agg, m.Mp(), tc.groupCount)
				filledSize := agg.Size()
				if sizeShouldIncrease {
					require.Greater(t, filledSize, grownSize, "Size() should increase after filling data")
				} else {
					// It might increase if internal structures are allocated on demand, but it must not decrease.
					require.GreaterOrEqual(t, filledSize, grownSize, "Size() should not decrease after filling data")
				}
			}

			agg.Free()
			require.Equal(t, before, m.Mp().CurrNB(), "memory leak detected in %s", tc.name)
		})
	}
}

func TestGroupConcatExecMarshalUnmarshal(t *testing.T) {
	m := hackAggMemoryManager()
	defer func() {
		require.Equal(t, int64(0), m.Mp().CurrNB())
	}()

	{
		info := multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			distinct:  false,
			argTypes:  []types.Type{types.T_varchar.ToType()},
			retType:   getCroupConcatRet(types.T_varchar.ToType()),
			emptyNull: true,
		}
		exec := newGroupConcatExec(m, info, ",")

		require.NoError(t, exec.GroupGrow(2))
		v1 := vector.NewVec(types.T_varchar.ToType())
		defer v1.Free(m.Mp())
		require.NoError(t, vector.AppendBytes(v1, []byte("test1"), false, m.Mp()))
		require.NoError(t, exec.Fill(0, 0, []*vector.Vector{v1}))

		data, err := exec.marshal()
		require.NoError(t, err)
		require.NotNil(t, data)

		newExec := newGroupConcatExec(m, info, ",")
		err = newExec.unmarshal(m.Mp(), nil, nil, [][]byte{[]byte(",")})
		require.NoError(t, err)

		require.Equal(t, []byte(","), newExec.(*groupConcatExec).separator)
		exec.Free()
		newExec.Free()
	}

	{
		info := multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			distinct:  false,
			argTypes:  []types.Type{types.T_varchar.ToType()},
			retType:   getCroupConcatRet(types.T_varchar.ToType()),
			emptyNull: true,
		}
		exec := newGroupConcatExec(m, info, "|")

		data, err := exec.marshal()
		require.NoError(t, err)

		newExec := newGroupConcatExec(m, info, ",")
		encoded := &EncodedAgg{}
		require.NoError(t, encoded.Unmarshal(data))

		err = newExec.unmarshal(m.Mp(), encoded.Result, encoded.Empties, encoded.Groups)
		require.NoError(t, err)

		require.Equal(t, []byte("|"), newExec.(*groupConcatExec).separator)
		exec.Free()
		newExec.Free()
	}

	{
		info := multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			distinct:  true,
			argTypes:  []types.Type{types.T_varchar.ToType()},
			retType:   getCroupConcatRet(types.T_varchar.ToType()),
			emptyNull: true,
		}
		exec := newGroupConcatExec(m, info, ",")
		require.NoError(t, exec.GroupGrow(1))

		v1 := vector.NewVec(types.T_varchar.ToType())
		defer v1.Free(m.Mp())
		require.NoError(t, vector.AppendBytes(v1, []byte("distinct1"), false, m.Mp()))
		require.NoError(t, exec.Fill(0, 0, []*vector.Vector{v1}))

		data, err := exec.marshal()
		require.NoError(t, err)

		newExec := newGroupConcatExec(m, info, ",")
		encoded := &EncodedAgg{}
		require.NoError(t, encoded.Unmarshal(data))

		err = newExec.unmarshal(m.Mp(), encoded.Result, encoded.Empties, encoded.Groups)
		require.NoError(t, err)

		exec.Free()
		newExec.Free()
	}

	{
		info := multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			distinct:  false,
			argTypes:  []types.Type{types.T_varchar.ToType()},
			retType:   getCroupConcatRet(types.T_varchar.ToType()),
			emptyNull: true,
		}
		exec := newGroupConcatExec(m, info, ",")

		exec.Free()
	}
}

func TestCountColumnExecMarshalUnmarshal(t *testing.T) {
	m := hackAggMemoryManager()
	defer func() {
		require.Equal(t, int64(0), m.Mp().CurrNB())
	}()

	{
		info := singleAggInfo{
			aggID:     AggIdOfCountColumn,
			distinct:  false,
			argType:   types.T_int64.ToType(),
			retType:   types.T_int64.ToType(),
			emptyNull: false,
		}
		exec := newCountColumnExecExec(m, info)
		require.NoError(t, exec.GroupGrow(3))

		v1 := vector.NewVec(types.T_int64.ToType())
		defer v1.Free(m.Mp())
		require.NoError(t, vector.AppendFixedList(v1, []int64{1, 2, 3}, nil, m.Mp()))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{v1}))

		data, err := exec.marshal()
		require.NoError(t, err)

		newExec := newCountColumnExecExec(m, info)
		encoded := &EncodedAgg{}
		require.NoError(t, encoded.Unmarshal(data))

		err = newExec.unmarshal(m.Mp(), encoded.Result, encoded.Empties, encoded.Groups)
		require.NoError(t, err)

		exec.Free()
		newExec.Free()
	}

	{
		info := singleAggInfo{
			aggID:     AggIdOfCountColumn,
			distinct:  true,
			argType:   types.T_int64.ToType(),
			retType:   types.T_int64.ToType(),
			emptyNull: false,
		}
		exec := newCountColumnExecExec(m, info)
		require.NoError(t, exec.GroupGrow(2))

		v1 := vector.NewVec(types.T_int64.ToType())
		defer v1.Free(m.Mp())
		require.NoError(t, vector.AppendFixedList(v1, []int64{1, 2, 1, 3}, nil, m.Mp()))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{v1}))

		data, err := exec.marshal()
		require.NoError(t, err)

		newExec := newCountColumnExecExec(m, info)
		encoded := &EncodedAgg{}
		require.NoError(t, encoded.Unmarshal(data))

		err = newExec.unmarshal(m.Mp(), encoded.Result, encoded.Empties, encoded.Groups)
		require.NoError(t, err)

		exec.Free()
		newExec.Free()
	}

	{
		info := singleAggInfo{
			aggID:     AggIdOfCountColumn,
			distinct:  true,
			argType:   types.T_int64.ToType(),
			retType:   types.T_int64.ToType(),
			emptyNull: false,
		}
		exec := newCountColumnExecExec(m, info)
		require.NoError(t, exec.GroupGrow(1))

		data, err := exec.marshal()
		require.NoError(t, err)

		newExec := newCountColumnExecExec(m, info)
		encoded := &EncodedAgg{}
		require.NoError(t, encoded.Unmarshal(data))

		err = newExec.unmarshal(m.Mp(), encoded.Result, encoded.Empties, encoded.Groups)
		require.NoError(t, err)

		exec.Free()
		newExec.Free()
	}

	{
		info := singleAggInfo{
			aggID:     AggIdOfCountColumn,
			distinct:  true,
			argType:   types.T_int64.ToType(),
			retType:   types.T_int64.ToType(),
			emptyNull: false,
		}
		exec := newCountColumnExecExec(m, info)

		err := exec.unmarshal(m.Mp(), nil, nil, [][]byte{})
		require.NoError(t, err)

		exec.Free()
	}
}

func TestDistinctHashMarshalUnmarshal(t *testing.T) {
	m := hackAggMemoryManager()
	defer func() {
		require.Equal(t, int64(0), m.Mp().CurrNB())
	}()

	{
		dh := newDistinctHash()

		data, err := dh.marshal()
		require.NoError(t, err)
		require.Nil(t, data)

		newDh := newDistinctHash()
		err = newDh.unmarshal(nil)
		require.NoError(t, err)

		require.Equal(t, 0, len(newDh.maps))
	}

	{
		dh := newDistinctHash()
		require.NoError(t, dh.grows(1))

		v1 := vector.NewVec(types.T_varchar.ToType())
		defer v1.Free(m.Mp())
		require.NoError(t, vector.AppendBytes(v1, []byte("key1"), false, m.Mp()))

		_, err := dh.fill(0, []*vector.Vector{v1}, 0)
		require.NoError(t, err)

		data, err := dh.marshal()
		require.NoError(t, err)
		require.NotNil(t, data)

		newDh := newDistinctHash()
		err = newDh.unmarshal(data)
		require.NoError(t, err)

		require.Equal(t, 1, len(newDh.maps))
		require.Equal(t, uint64(1), newDh.maps[0].GroupCount())

		v1.Free(m.Mp())
	}

	{
		dh := newDistinctHash()
		require.NoError(t, dh.grows(3))

		v1 := vector.NewVec(types.T_varchar.ToType())
		defer v1.Free(m.Mp())
		require.NoError(t, vector.AppendBytes(v1, []byte("key1"), false, m.Mp()))

		v2 := vector.NewVec(types.T_varchar.ToType())
		defer v2.Free(m.Mp())
		require.NoError(t, vector.AppendBytes(v2, []byte("key2"), false, m.Mp()))

		_, err := dh.fill(0, []*vector.Vector{v1}, 0)
		require.NoError(t, err)

		_, err = dh.fill(1, []*vector.Vector{v2}, 0)
		require.NoError(t, err)

		data, err := dh.marshal()
		require.NoError(t, err)
		require.NotNil(t, data)

		newDh := newDistinctHash()
		err = newDh.unmarshal(data)
		require.NoError(t, err)

		require.Equal(t, 3, len(newDh.maps))
		require.Equal(t, uint64(1), newDh.maps[0].GroupCount())
		require.Equal(t, uint64(1), newDh.maps[1].GroupCount())
		require.Equal(t, uint64(0), newDh.maps[2].GroupCount())

		v1.Free(m.Mp())
		v2.Free(m.Mp())
	}

}
