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

package colexec

import (
	"context"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	util2 "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType          = types.T_bool.ToType()
	constI8Type         = types.T_int8.ToType()
	constI16Type        = types.T_int16.ToType()
	constI32Type        = types.T_int32.ToType()
	constI64Type        = types.T_int64.ToType()
	constU8Type         = types.T_uint8.ToType()
	constU16Type        = types.T_uint16.ToType()
	constU32Type        = types.T_uint32.ToType()
	constU64Type        = types.T_uint64.ToType()
	constFType          = types.T_float32.ToType()
	constDType          = types.T_float64.ToType()
	constSType          = types.T_varchar.ToType()
	constBinType        = types.T_varbinary.ToType()
	constDateType       = types.T_date.ToType()
	constTimeType       = types.T_time.ToType()
	constDatetimeType   = types.T_datetime.ToType()
	constEnumType       = types.T_enum.ToType()
	constTimestampTypes = []types.Type{
		types.New(types.T_timestamp, 0, 0),
		types.New(types.T_timestamp, 0, 1),
		types.New(types.T_timestamp, 0, 2),
		types.New(types.T_timestamp, 0, 3),
		types.New(types.T_timestamp, 0, 4),
		types.New(types.T_timestamp, 0, 5),
		types.New(types.T_timestamp, 0, 6),
	}
	//No need to add T_array here, as Array is cast from varchar.
)

// ExpressionExecutor
// generated from plan.Expr, can evaluate the result from vectors directly.
type ExpressionExecutor interface {
	// Eval evaluates the expression and returns the result vector.
	// The result is a read-only vector that is only valid until the next call to Eval.
	Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error)

	// EvalWithoutResultReusing will be removed in the future.
	// It's used to evaluate the expression,
	// and the result's ownership is transferred to the caller except for the column expression.
	EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error)

	// ResetForNextQuery resets the expression to its initial state for a same expression.
	// this is useful to a prepare statement.
	ResetForNextQuery()

	// Free closes the expression and releases all resources.
	Free()

	// IsColumnExpr returns true if the expression is a column expression.
	IsColumnExpr() bool

	TypeName() string
}

func NewExpressionExecutorsFromPlanExpressions(proc *process.Process, planExprs []*plan.Expr) (executors []ExpressionExecutor, err error) {
	executors = make([]ExpressionExecutor, len(planExprs))
	for i := range executors {
		executors[i], err = NewExpressionExecutor(proc, planExprs[i])
		if err != nil {
			for j := 0; j < i; j++ {
				executors[j].Free()
			}
			return nil, err
		}
	}
	return executors, err
}

func NewExpressionExecutor(proc *process.Process, planExpr *plan.Expr) (ExpressionExecutor, error) {
	switch t := planExpr.Expr.(type) {
	case *plan.Expr_Lit:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		vec, err := generateConstExpressionExecutor(proc, typ, t.Lit)
		if err != nil {
			return nil, err
		}
		return NewFixedVectorExpressionExecutor(proc.Mp(), false, vec), nil

	case *plan.Expr_T:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		vec := vector.NewConstNull(typ, 1, proc.Mp())
		return NewFixedVectorExpressionExecutor(proc.Mp(), false, vec), nil

	case *plan.Expr_Col:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		ce := NewColumnExpressionExecutor()
		*ce = ColumnExpressionExecutor{
			mp:       proc.Mp(),
			relIndex: int(t.Col.RelPos),
			colIndex: int(t.Col.ColPos),
			typ:      typ,
		}
		// [issue#19574]
		// if < 0, it's special for agg or others.
		if ce.relIndex < 0 {
			ce.relIndex = 0
		}
		return ce, nil

	case *plan.Expr_P:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		return NewParamExpressionExecutor(proc.Mp(), int(t.P.Pos), typ), nil

	case *plan.Expr_V:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		ve := NewVarExpressionExecutor()
		*ve = VarExpressionExecutor{
			mp:     proc.Mp(),
			name:   t.V.Name,
			system: t.V.System,
			global: t.V.Global,
			typ:    typ,
		}
		return ve, nil

	case *plan.Expr_Vec:
		vec := vector.NewVec(types.T_any.ToType())
		err := vec.UnmarshalBinary(t.Vec.Data)
		if err != nil {
			return nil, err
		}
		return NewFixedVectorExpressionExecutor(proc.Mp(), true, vec), nil

	case *plan.Expr_List:
		executor := NewListExpressionExecutor()
		resultVecTyp := t.List.List[0].GetTyp()
		typ := types.New(types.T(resultVecTyp.Id), resultVecTyp.Width, resultVecTyp.Scale)
		executor.Init(proc, typ, len(t.List.List))
		for i := range executor.parameterExecutor {
			subExecutor, paramErr := NewExpressionExecutor(proc, t.List.List[i])
			if paramErr != nil {
				executor.Free()
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}
		return executor, nil

	case *plan.Expr_F:
		overloadID := t.F.GetFunc().GetObj()
		overload, err := function.GetFunctionById(proc.Ctx, overloadID)
		if err != nil {
			return nil, err
		}

		executor := NewFunctionExpressionExecutor()
		{
			// init function folding status.
			executor.folded.reset(proc.Mp())
		}
		{
			// init function information for evaluation.
			executor.overloadID = overloadID
			executor.volatile, executor.timeDependent = overload.CannotFold(), overload.IsRealTimeRelated()
			executor.fid, _ = function.DecodeOverloadID(overloadID)
			executor.evalFn, executor.resetFn, executor.freeFn = overload.GetExecuteMethod()
		}
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)

		if err = executor.Init(proc, len(t.F.Args), typ); err != nil {
			executor.Free()
			return nil, err
		}

		for i := range executor.parameterExecutor {
			subExecutor, paramErr := NewExpressionExecutor(proc, t.F.Args[i])
			if paramErr != nil {
				executor.Free()
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}
		return executor, nil
	}

	return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported expression executor for %v now", planExpr))
}

// FixedVectorExpressionExecutor
// the content of its vector is fixed.
// e.g.
//
//	ConstVector [1, 1, 1, 1, 1]
//	ConstVector [null, null, null]
//	ListVector  ["1", "2", "3", null, "5"]
type FixedVectorExpressionExecutor struct {
	m *mpool.MPool

	noNeedToSetLength bool
	resultVector      *vector.Vector
}

type FunctionExpressionExecutor struct {
	m *mpool.MPool
	functionInformationForEval
	folded      functionFolding
	selectList1 []bool
	selectList2 []bool
	selectList  function.FunctionSelectList

	resultVector vector.FunctionResultWrapper
	// parameters related
	parameterResults  []*vector.Vector
	parameterExecutor []ExpressionExecutor
}

type ColumnExpressionExecutor struct {
	mp       *mpool.MPool
	relIndex int
	colIndex int

	// result type.
	typ types.Type
	// we should new and cache a null vector here.
	// because we need to change its type when doing the execution for const null vector.
	// but other process may using its type at the same time.
	nullVecCache *vector.Vector
}

func (expr *ColumnExpressionExecutor) GetRelIndex() int {
	return expr.relIndex
}

func (expr *ColumnExpressionExecutor) GetColIndex() int {
	return expr.colIndex
}

type ParamExpressionExecutor struct {
	mp   *mpool.MPool
	null *vector.Vector
	vec  *vector.Vector
	pos  int
	typ  types.Type

	folded bool
}

func (expr *ParamExpressionExecutor) Eval(proc *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if expr.folded {
		if expr.null != nil {
			return expr.null, nil
		}
		if expr.vec != nil {
			return expr.vec, nil
		}
	}

	expr.folded = true
	val, err := proc.GetPrepareParamsAt(expr.pos)
	if err != nil {
		return nil, err
	}

	if val == nil {
		if expr.null == nil {
			expr.null = vector.NewConstNull(expr.typ, 1, proc.GetMPool())
		}
		return expr.null, nil
	}

	if expr.vec == nil {
		expr.vec, err = vector.NewConstBytes(expr.typ, val, 1, proc.Mp())
	} else {
		err = vector.SetConstBytes(expr.vec, val, 1, proc.GetMPool())
	}
	return expr.vec, err
}

func (expr *ParamExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if vec == expr.null {
		expr.null = nil
		return vec, nil
	}
	expr.vec = nil
	return vec, nil
}

func (expr *ParamExpressionExecutor) Free() {
	if expr == nil {
		return
	}

	if expr.vec != nil {
		expr.vec.Free(expr.mp)
		expr.vec = nil
	}
	if expr.null != nil {
		expr.null.Free(expr.mp)
		expr.null = nil
	}
	reuse.Free[ParamExpressionExecutor](expr, nil)
}

func (expr *ParamExpressionExecutor) IsColumnExpr() bool {
	return false
}

type VarExpressionExecutor struct {
	mp   *mpool.MPool
	null *vector.Vector
	vec  *vector.Vector

	name   string
	system bool
	global bool
	typ    types.Type
}

func (expr *VarExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	val, err := proc.GetResolveVariableFunc()(expr.name, expr.system, expr.global)
	if err != nil {
		return nil, err
	}

	if val == nil {
		if expr.null == nil {
			expr.null, err = util.GenVectorByVarValue(proc, expr.typ, nil)
		}
		return expr.null, err
	}

	if expr.vec == nil {
		expr.vec, err = util.GenVectorByVarValue(proc, expr.typ, val)
	} else {
		switch v := val.(type) {
		case []byte:
			err = vector.SetConstBytes(expr.vec, v, 1, proc.GetMPool())
		case string:
			err = vector.SetConstBytes(expr.vec, util2.UnsafeStringToBytes(v), 1, proc.GetMPool())
		default:
			err = vector.SetConstBytes(expr.vec, util2.UnsafeStringToBytes(fmt.Sprintf("%v", v)), 1, proc.GetMPool())
		}
	}
	return expr.vec, err
}

func (expr *VarExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if vec == expr.null {
		expr.null = nil
		return vec, nil
	}
	expr.vec = nil
	return vec, nil
}

func (expr *VarExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.vec != nil {
		expr.vec.Free(expr.mp)
		expr.vec = nil
	}
	if expr.null != nil {
		expr.null.Free(expr.mp)
		expr.null = nil
	}
	reuse.Free[VarExpressionExecutor](expr, nil)
}

func (expr *VarExpressionExecutor) IsColumnExpr() bool {
	return false
}

type ListExpressionExecutor struct {
	mp *mpool.MPool

	typ               types.Type
	resultVector      *vector.Vector
	parameterExecutor []ExpressionExecutor
}

func (expr *ListExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {

	if expr.resultVector == nil {
		expr.resultVector = vector.NewVec(expr.typ)
	} else {
		expr.resultVector.CleanOnlyData()
	}
	for i := range expr.parameterExecutor {
		vec, err := expr.parameterExecutor[i].Eval(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
		err = expr.resultVector.UnionOne(vec, 0, expr.mp)
		if err != nil {
			return nil, err
		}
	}
	expr.resultVector.SetLength(len(expr.parameterExecutor))
	return expr.resultVector, nil
}

func (expr *ListExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	expr.resultVector = nil
	return vec, nil
}

func (expr *ListExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	for _, e := range expr.parameterExecutor {
		e.Free()
	}
	if expr.resultVector != nil {
		expr.resultVector.Free(expr.mp)
		expr.resultVector = nil
	}
	reuse.Free[ListExpressionExecutor](expr, nil)
}

func (expr *ListExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (expr *ListExpressionExecutor) Init(proc *process.Process, typ types.Type, parameterNum int) {
	m := proc.Mp()

	expr.typ = typ
	expr.mp = m
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)
	expr.resultVector = vector.NewVec(typ)
}

func (expr *ListExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *ListExpressionExecutor) ResetForNextQuery() {
	for _, e := range expr.parameterExecutor {
		e.ResetForNextQuery()
	}
}

func (expr *FunctionExpressionExecutor) Init(
	proc *process.Process,
	parameterNum int,
	retType types.Type) (err error) {
	m := proc.Mp()

	expr.m = m
	expr.parameterResults = make([]*vector.Vector, parameterNum)
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)

	expr.resultVector = vector.NewFunctionResultWrapper(retType, m)
	return err
}

func (expr *FunctionExpressionExecutor) EvalIff(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	expr.parameterResults[0], err = expr.parameterExecutor[0].Eval(proc, batches, selectList)
	if err != nil {
		return err
	}
	rowCount := batches[0].RowCount()
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
		expr.selectList2 = make([]bool, rowCount)
	}

	bs := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[0])
	for i := 0; i < rowCount; i++ {
		b, null := bs.GetValue(uint64(i))
		if selectList != nil {
			expr.selectList1[i] = selectList[i]
			expr.selectList2[i] = selectList[i]
		} else {
			expr.selectList1[i] = true
			expr.selectList2[i] = true
		}
		if !null && b {
			expr.selectList2[i] = false
		} else {
			expr.selectList1[i] = false
		}
	}
	expr.parameterResults[1], err = expr.parameterExecutor[1].Eval(proc, batches, expr.selectList1)
	if err != nil {
		return err
	}
	expr.parameterResults[2], err = expr.parameterExecutor[2].Eval(proc, batches, expr.selectList2)
	return err
}

func (expr *FunctionExpressionExecutor) EvalCase(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	rowCount := batches[0].RowCount()
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
		expr.selectList2 = make([]bool, rowCount)
	}
	if selectList != nil {
		copy(expr.selectList1, selectList)
	} else {
		for i := range expr.selectList1 {
			expr.selectList1[i] = true
		}
	}
	for i := 0; i < len(expr.parameterExecutor); i += 2 {
		expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, expr.selectList1)
		if err != nil {
			return err
		}
		if i != len(expr.parameterExecutor)-1 {
			bs := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[i])

			for j := 0; j < rowCount; j++ {
				b, null := bs.GetValue(uint64(j))
				if !null && b {
					expr.selectList1[j] = false
					expr.selectList2[j] = true
				} else {
					expr.selectList2[j] = false
				}
			}
			expr.parameterResults[i+1], err = expr.parameterExecutor[i+1].Eval(proc, batches, expr.selectList2)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (expr *FunctionExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	if expr.folded.needFoldingCheck {
		if err := expr.doFold(proc, proc.GetBaseProcessRunningStatus()); err != nil {
			return nil, err
		}
	}
	if expr.folded.canFold {
		if len(batches) > 0 {
			return expr.getFoldedVector(batches[0].RowCount()), nil
		}
		return expr.getFoldedVector(1), nil
	}

	var err error
	if expr.fid == function.IFF {
		err = expr.EvalIff(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else if expr.fid == function.CASE {
		err = expr.EvalCase(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else {
		for i := range expr.parameterExecutor {
			expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, selectList)
			if err != nil {
				return nil, err
			}
		}
	}

	if err = expr.resultVector.PreExtendAndReset(batches[0].RowCount()); err != nil {
		return nil, err
	}

	if selectList != nil && len(expr.selectList.SelectList) < batches[0].RowCount() {
		expr.selectList.SelectList = make([]bool, batches[0].RowCount())
	}
	if selectList == nil {
		expr.selectList.AnyNull = false
		expr.selectList.AllNull = false
		for i := range expr.selectList.SelectList {
			expr.selectList.SelectList[i] = true
		}
	} else {
		expr.selectList.AllNull = true
		expr.selectList.AnyNull = false
		for i := range selectList {
			expr.selectList.SelectList[i] = selectList[i]
			if selectList[i] {
				expr.selectList.AllNull = false
			} else {
				expr.selectList.AnyNull = true
			}
		}
	}

	if err = expr.evalFn(
		expr.parameterResults, expr.resultVector, proc, batches[0].RowCount(), &expr.selectList); err != nil {
		return nil, err
	}

	return expr.resultVector.GetResultVector(), nil
}

func (expr *FunctionExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if expr.folded.canFold {
		return vec.Dup(proc.Mp())
	}
	expr.resultVector.SetResultVector(nil)
	return vec, nil
}

func (expr *FunctionExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.resultVector != nil {
		expr.resultVector.Free()
		expr.resultVector = nil
	}

	for _, p := range expr.parameterExecutor {
		if p != nil {
			p.Free()
		}
	}
	if expr.freeFn != nil {
		_ = expr.freeFn()
		expr.freeFn = nil
	}
	reuse.Free[FunctionExpressionExecutor](expr, nil)
}

func (expr *FunctionExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *FunctionExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (expr *ColumnExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {

	relIndex := expr.relIndex
	// XXX it's a bad hack here. root cause is pipeline set a wrong relation index here.
	if len(batches) == 1 {
		relIndex = 0
	}

	vec := batches[relIndex].Vecs[expr.colIndex]
	if vec.IsConstNull() {
		vec = expr.getConstNullVec(expr.typ, vec.Length())
	}
	return vec, nil
}

func (expr *ColumnExpressionExecutor) getConstNullVec(typ types.Type, length int) *vector.Vector {
	if expr.nullVecCache != nil {
		expr.nullVecCache.SetType(typ)
		expr.nullVecCache.SetLength(length)
	} else {
		expr.nullVecCache = vector.NewConstNull(typ, length, expr.mp)
	}
	return expr.nullVecCache
}

func (expr *ColumnExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if vec == expr.nullVecCache {
		expr.nullVecCache = nil
	}
	return vec, err
}

func (expr *ColumnExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.nullVecCache != nil {
		expr.nullVecCache.Free(expr.mp)
		expr.nullVecCache = nil
	}
	reuse.Free[ColumnExpressionExecutor](expr, nil)
}

func (expr *ColumnExpressionExecutor) IsColumnExpr() bool {
	return true
}

func (expr *FixedVectorExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if !expr.noNeedToSetLength {
		expr.resultVector.SetLength(batches[0].RowCount())
	}
	return expr.resultVector, nil
}

func (expr *FixedVectorExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	return vec.Dup(proc.Mp())
}

func (expr *FixedVectorExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	defer reuse.Free[FixedVectorExpressionExecutor](expr, nil)
	if expr.resultVector == nil {
		return
	}
	expr.resultVector.Free(expr.m)
	expr.resultVector = nil
}

func (expr *FixedVectorExpressionExecutor) IsColumnExpr() bool {
	return false
}

func generateConstExpressionExecutor(proc *process.Process, typ types.Type, con *plan.Literal) (vec *vector.Vector, err error) {
	if con.GetIsnull() {
		vec = vector.NewConstNull(typ, 1, proc.Mp())
	} else {
		switch val := con.GetValue().(type) {
		case *plan.Literal_Bval:
			vec, err = vector.NewConstFixed(constBType, val.Bval, 1, proc.Mp())
		case *plan.Literal_I8Val:
			vec, err = vector.NewConstFixed(constI8Type, int8(val.I8Val), 1, proc.Mp())
		case *plan.Literal_I16Val:
			vec, err = vector.NewConstFixed(constI16Type, int16(val.I16Val), 1, proc.Mp())
		case *plan.Literal_I32Val:
			vec, err = vector.NewConstFixed(constI32Type, val.I32Val, 1, proc.Mp())
		case *plan.Literal_I64Val:
			vec, err = vector.NewConstFixed(constI64Type, val.I64Val, 1, proc.Mp())
		case *plan.Literal_U8Val:
			vec, err = vector.NewConstFixed(constU8Type, uint8(val.U8Val), 1, proc.Mp())
		case *plan.Literal_U16Val:
			vec, err = vector.NewConstFixed(constU16Type, uint16(val.U16Val), 1, proc.Mp())
		case *plan.Literal_U32Val:
			vec, err = vector.NewConstFixed(constU32Type, val.U32Val, 1, proc.Mp())
		case *plan.Literal_U64Val:
			vec, err = vector.NewConstFixed(constU64Type, val.U64Val, 1, proc.Mp())
		case *plan.Literal_Fval:
			vec, err = vector.NewConstFixed(constFType, val.Fval, 1, proc.Mp())
		case *plan.Literal_Dval:
			vec, err = vector.NewConstFixed(constDType, val.Dval, 1, proc.Mp())
		case *plan.Literal_Dateval:
			vec, err = vector.NewConstFixed(constDateType, types.Date(val.Dateval), 1, proc.Mp())
		case *plan.Literal_Timeval:
			vec, err = vector.NewConstFixed(typ, types.Time(val.Timeval), 1, proc.Mp())
		case *plan.Literal_Datetimeval:
			vec, err = vector.NewConstFixed(typ, types.Datetime(val.Datetimeval), 1, proc.Mp())
		case *plan.Literal_Decimal64Val:
			cd64 := val.Decimal64Val
			d64 := types.Decimal64(cd64.A)
			vec, err = vector.NewConstFixed(typ, d64, 1, proc.Mp())
		case *plan.Literal_Decimal128Val:
			cd128 := val.Decimal128Val
			d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
			vec, err = vector.NewConstFixed(typ, d128, 1, proc.Mp())
		case *plan.Literal_Timestampval:
			scale := typ.Scale
			if scale < 0 || scale > 6 {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
			}
			vec, err = vector.NewConstFixed(constTimestampTypes[scale], types.Timestamp(val.Timestampval), 1, proc.Mp())
		case *plan.Literal_Sval:
			sval := val.Sval
			// Distinguish binary with non-binary string.
			if typ.Oid == types.T_binary || typ.Oid == types.T_varbinary || typ.Oid == types.T_blob {
				vec, err = vector.NewConstBytes(constBinType, []byte(sval), 1, proc.Mp())
			} else if typ.Oid == types.T_array_float32 {
				array, err1 := types.StringToArray[float32](sval)
				if err1 != nil {
					return nil, err1
				}
				vec, err = vector.NewConstArray(typ, array, 1, proc.Mp())
			} else if typ.Oid == types.T_array_float64 {
				array, err1 := types.StringToArray[float64](sval)
				if err1 != nil {
					return nil, err1
				}
				vec, err = vector.NewConstArray(typ, array, 1, proc.Mp())
			} else if typ.Oid == types.T_datalink {
				_, _, err1 := datalink.ParseDatalink(sval, proc)
				if err1 != nil {
					return nil, err1
				}
				vec, err = vector.NewConstBytes(constBinType, []byte(sval), 1, proc.Mp())
			} else {
				vec, err = vector.NewConstBytes(constSType, []byte(sval), 1, proc.Mp())
			}
		case *plan.Literal_Defaultval:
			defaultVal := val.Defaultval
			vec, err = vector.NewConstFixed(constBType, defaultVal, 1, proc.Mp())
		case *plan.Literal_EnumVal:
			vec, err = vector.NewConstFixed(constEnumType, types.Enum(val.EnumVal), 1, proc.Mp())
		default:
			return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", con.GetValue()))
		}
		vec.SetIsBin(con.IsBin)
	}
	return vec, err
}

func GenerateConstListExpressionExecutor(proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	lenList := len(exprs)
	vec, err := proc.AllocVectorOfRows(types.New(types.T(exprs[0].Typ.Id), exprs[0].Typ.Width, exprs[0].Typ.Scale), lenList, nil)
	if err != nil {
		return nil, err
	}
	for i := 0; i < lenList; i++ {
		expr := exprs[i]
		t := expr.GetLit()
		if t == nil {
			return nil, moerr.NewInternalError(proc.Ctx, "args in list must be constant")
		}
		if t.GetIsnull() {
			vec.GetNulls().Set(uint64(i))
		} else {
			switch val := t.GetValue().(type) {
			case *plan.Literal_Bval:
				veccol := vector.MustFixedColNoTypeCheck[bool](vec)
				veccol[i] = val.Bval
			case *plan.Literal_I8Val:
				veccol := vector.MustFixedColNoTypeCheck[int8](vec)
				veccol[i] = int8(val.I8Val)
			case *plan.Literal_I16Val:
				veccol := vector.MustFixedColNoTypeCheck[int16](vec)
				veccol[i] = int16(val.I16Val)
			case *plan.Literal_I32Val:
				veccol := vector.MustFixedColNoTypeCheck[int32](vec)
				veccol[i] = val.I32Val
			case *plan.Literal_I64Val:
				veccol := vector.MustFixedColNoTypeCheck[int64](vec)
				veccol[i] = val.I64Val
			case *plan.Literal_U8Val:
				veccol := vector.MustFixedColNoTypeCheck[uint8](vec)
				veccol[i] = uint8(val.U8Val)
			case *plan.Literal_U16Val:
				veccol := vector.MustFixedColNoTypeCheck[uint16](vec)
				veccol[i] = uint16(val.U16Val)
			case *plan.Literal_U32Val:
				veccol := vector.MustFixedColNoTypeCheck[uint32](vec)
				veccol[i] = val.U32Val
			case *plan.Literal_U64Val:
				veccol := vector.MustFixedColNoTypeCheck[uint64](vec)
				veccol[i] = val.U64Val
			case *plan.Literal_Fval:
				veccol := vector.MustFixedColNoTypeCheck[float32](vec)
				veccol[i] = val.Fval
			case *plan.Literal_Dval:
				veccol := vector.MustFixedColNoTypeCheck[float64](vec)
				veccol[i] = val.Dval
			case *plan.Literal_Dateval:
				veccol := vector.MustFixedColNoTypeCheck[types.Date](vec)
				veccol[i] = types.Date(val.Dateval)
			case *plan.Literal_Timeval:
				veccol := vector.MustFixedColNoTypeCheck[types.Time](vec)
				veccol[i] = types.Time(val.Timeval)
			case *plan.Literal_Datetimeval:
				veccol := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
				veccol[i] = types.Datetime(val.Datetimeval)
			case *plan.Literal_Decimal64Val:
				cd64 := val.Decimal64Val
				d64 := types.Decimal64(cd64.A)
				veccol := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
				veccol[i] = d64
			case *plan.Literal_Decimal128Val:
				cd128 := val.Decimal128Val
				d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
				veccol := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
				veccol[i] = d128
			case *plan.Literal_Timestampval:
				scale := expr.Typ.Scale
				if scale < 0 || scale > 6 {
					return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
				}
				veccol := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
				veccol[i] = types.Timestamp(val.Timestampval)
			case *plan.Literal_Sval:
				sval := val.Sval
				err = vector.SetStringAt(vec, i, sval, proc.Mp())
				if err != nil {
					return nil, err
				}
			case *plan.Literal_Defaultval:
				defaultVal := val.Defaultval
				veccol := vector.MustFixedColNoTypeCheck[bool](vec)
				veccol[i] = defaultVal
			case *plan.Literal_EnumVal:
				veccol := vector.MustFixedColNoTypeCheck[types.Enum](vec)
				veccol[i] = types.Enum(val.EnumVal)
			default:
				return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", t.GetValue()))
			}
			vec.SetIsBin(t.IsBin)
		}
	}
	return vec, nil
}

func NewJoinBatch(bat *batch.Batch, mp *mpool.MPool) (*batch.Batch,
	[]func(*vector.Vector, *vector.Vector, int64, int) error) {
	rbat := batch.NewWithSize(bat.VectorCount())
	cfs := make([]func(*vector.Vector, *vector.Vector, int64, int) error, bat.VectorCount())
	for i, vec := range bat.Vecs {
		typ := *vec.GetType()
		rbat.Vecs[i] = vector.NewConstNull(typ, 0, nil)
		cfs[i] = vector.GetConstSetFunction(typ, mp)
	}
	return rbat, cfs
}

func SetJoinBatchValues(joinBat, bat *batch.Batch, sel int64, length int,
	cfs []func(*vector.Vector, *vector.Vector, int64, int) error) error {
	for i, vec := range bat.Vecs {
		if err := cfs[i](joinBat.Vecs[i], vec, sel, length); err != nil {
			return err
		}
	}
	joinBat.SetRowCount(length)
	return nil
}

func getConstZM(
	ctx context.Context,
	expr *plan.Expr,
	proc *process.Process,
) (zm index.ZM, err error) {
	c := expr.GetLit()
	typ := expr.Typ
	if c.GetIsnull() {
		zm = index.NewZM(types.T(typ.Id), typ.Scale)
		return
	}
	switch val := c.GetValue().(type) {
	case *plan.Literal_Bval:
		zm = index.NewZM(constBType.Oid, 0)
		v := val.Bval
		index.UpdateZM(zm, types.EncodeBool(&v))
	case *plan.Literal_I8Val:
		zm = index.NewZM(constI8Type.Oid, 0)
		v := int8(val.I8Val)
		index.UpdateZM(zm, types.EncodeInt8(&v))
	case *plan.Literal_I16Val:
		zm = index.NewZM(constI16Type.Oid, 0)
		v := int16(val.I16Val)
		index.UpdateZM(zm, types.EncodeInt16(&v))
	case *plan.Literal_I32Val:
		zm = index.NewZM(constI32Type.Oid, 0)
		v := val.I32Val
		index.UpdateZM(zm, types.EncodeInt32(&v))
	case *plan.Literal_I64Val:
		zm = index.NewZM(constI64Type.Oid, 0)
		v := val.I64Val
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_U8Val:
		zm = index.NewZM(constU8Type.Oid, 0)
		v := uint8(val.U8Val)
		index.UpdateZM(zm, types.EncodeUint8(&v))
	case *plan.Literal_U16Val:
		zm = index.NewZM(constU16Type.Oid, 0)
		v := uint16(val.U16Val)
		index.UpdateZM(zm, types.EncodeUint16(&v))
	case *plan.Literal_U32Val:
		zm = index.NewZM(constU32Type.Oid, 0)
		v := val.U32Val
		index.UpdateZM(zm, types.EncodeUint32(&v))
	case *plan.Literal_U64Val:
		zm = index.NewZM(constU64Type.Oid, 0)
		v := val.U64Val
		index.UpdateZM(zm, types.EncodeUint64(&v))
	case *plan.Literal_Fval:
		zm = index.NewZM(constFType.Oid, 0)
		v := val.Fval
		index.UpdateZM(zm, types.EncodeFloat32(&v))
	case *plan.Literal_Dval:
		zm = index.NewZM(constDType.Oid, 0)
		v := val.Dval
		index.UpdateZM(zm, types.EncodeFloat64(&v))
	case *plan.Literal_Dateval:
		zm = index.NewZM(constDateType.Oid, 0)
		v := val.Dateval
		index.UpdateZM(zm, types.EncodeInt32(&v))
	case *plan.Literal_Timeval:
		zm = index.NewZM(constTimeType.Oid, 0)
		v := val.Timeval
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Datetimeval:
		zm = index.NewZM(constDatetimeType.Oid, 0)
		v := val.Datetimeval
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Decimal64Val:
		v := val.Decimal64Val
		zm = index.NewZM(types.T_decimal64, typ.Scale)
		d64 := types.Decimal64(v.A)
		index.UpdateZM(zm, types.EncodeDecimal64(&d64))
	case *plan.Literal_Decimal128Val:
		v := val.Decimal128Val
		zm = index.NewZM(types.T_decimal128, typ.Scale)
		d128 := types.Decimal128{B0_63: uint64(v.A), B64_127: uint64(v.B)}
		index.UpdateZM(zm, types.EncodeDecimal128(&d128))
	case *plan.Literal_Timestampval:
		v := val.Timestampval
		scale := typ.Scale
		if scale < 0 || scale > 6 {
			err = moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
			return
		}
		zm = index.NewZM(constTimestampTypes[0].Oid, scale)
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Sval:
		zm = index.NewZM(constSType.Oid, 0)
		v := val.Sval
		index.UpdateZM(zm, []byte(v))
	case *plan.Literal_Defaultval:
		zm = index.NewZM(constBType.Oid, 0)
		v := val.Defaultval
		index.UpdateZM(zm, types.EncodeBool(&v))
	case *plan.Literal_EnumVal:
		zm = index.NewZM(constEnumType.Oid, 0)
		v := types.Enum(val.EnumVal)
		index.UpdateZM(zm, types.EncodeEnum(&v))
	default:
		err = moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", c.GetValue()))
	}
	return
}

func EvaluateFilterByZoneMap(
	ctx context.Context, // why we need a context here, to escape trace?
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (selected bool) {
	if expr == nil {
		selected = true
		return
	}

	if len(columnMap) == 0 {
		vec, free, err := GetReadonlyResultFromNoColumnExpression(proc, expr)
		if err != nil {
			return true
		}
		cols := vector.MustFixedColWithTypeCheck[bool](vec)
		for _, isNeed := range cols {
			if isNeed {
				free()
				return true
			}
		}
		free()
		return false
	}

	zm := GetExprZoneMap(ctx, proc, expr, meta, columnMap, zms, vecs)
	if !zm.IsInited() || zm.GetType() != types.T_bool {
		selected = false
	} else {
		selected = types.DecodeBool(zm.GetMaxBuf())
	}

	// clean the vector.
	for i := range vecs {
		if vecs[i] != nil {
			vecs[i].Free(proc.Mp())
			vecs[i] = nil
		}
	}
	return
}

func GetExprZoneMap(
	ctx context.Context,
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (v objectio.ZoneMap) {
	var err error

	switch t := expr.Expr.(type) {
	case *plan.Expr_Lit:
		if zms[expr.AuxId] == nil {
			if zms[expr.AuxId], err = getConstZM(ctx, expr, proc); err != nil {
				zms[expr.AuxId] = objectio.NewZM(types.T_bool, 0)
			}
		}

	case *plan.Expr_Col:
		zms[expr.AuxId] = meta.MustGetColumn(uint16(columnMap[int(t.Col.ColPos)])).ZoneMap()

	case *plan.Expr_F:
		id := t.F.GetFunc().GetObj()
		if overload, errGetFunc := function.GetFunctionById(ctx, id); errGetFunc != nil {
			zms[expr.AuxId].Reset()

		} else {
			args := t.F.Args

			// Some expressions need to be handled specifically
			switch t.F.Func.ObjName {
			case "isnull", "is_null":
				switch exprImpl := args[0].Expr.(type) {
				case *plan.Expr_Col:
					nullCnt := meta.MustGetColumn(uint16(columnMap[int(exprImpl.Col.ColPos)])).NullCnt()
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], nullCnt > 0)
					return zms[expr.AuxId]
				default:
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
			case "isnotnull", "is_not_null":
				switch exprImpl := args[0].Expr.(type) {
				case *plan.Expr_Col:
					zm := meta.MustGetColumn(uint16(columnMap[int(exprImpl.Col.ColPos)])).ZoneMap()
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], zm.IsInited())
					return zms[expr.AuxId]
				default:
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
			case "in":
				rid := args[1].AuxId
				if vecs[rid] == nil {
					if data, ok := args[1].Expr.(*plan.Expr_Vec); ok {
						vec := vector.NewVec(types.T_any.ToType())
						vec.UnmarshalBinary(data.Vec.Data)
						vecs[rid] = vec
					} else {
						zms[expr.AuxId].Reset()
						vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
						return zms[expr.AuxId]
					}
				}

				if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.AnyIn(vecs[rid]))
				return zms[expr.AuxId]

			case "prefix_eq":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				s := []byte(args[1].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixEq(s))
				return zms[expr.AuxId]

			case "prefix_between":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lb := []byte(args[1].GetLit().GetSval())
				ub := []byte(args[2].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixBetween(lb, ub))
				return zms[expr.AuxId]

			case "prefix_in":
				rid := args[1].AuxId
				if vecs[rid] == nil {
					if data, ok := args[1].Expr.(*plan.Expr_Vec); ok {
						vec := vector.NewVec(types.T_any.ToType())
						vec.UnmarshalBinary(data.Vec.Data)
						vecs[rid] = vec
					} else {
						zms[expr.AuxId].Reset()
						vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
						return zms[expr.AuxId]
					}
				}

				if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixIn(vecs[rid]))
				return zms[expr.AuxId]
			}

			f := func() bool {
				for _, arg := range args {
					zms[arg.AuxId] = GetExprZoneMap(ctx, proc, arg, meta, columnMap, zms, vecs)
					if !zms[arg.AuxId].IsInited() {
						zms[expr.AuxId].Reset()
						return true
					}
				}
				return false
			}

			var res, ok bool
			switch t.F.Func.ObjName {
			case ">":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyGT(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "<":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyLT(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case ">=":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyGE(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "<=":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyLE(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "=":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].Intersect(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "between":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyBetween(zms[args[1].AuxId], zms[args[2].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "and":
				if f() {
					return zms[expr.AuxId]
				}
				zmRes := zms[args[0].AuxId]
				for i := 1; i < len(args); i++ {
					if res, ok = zmRes.And(zms[args[i].AuxId]); !ok {
						zmRes.Reset()
						break
					} else {
						zmRes = index.SetBool(zmRes, res)
					}
				}
				zms[expr.AuxId] = zmRes

			case "or":
				if f() {
					return zms[expr.AuxId]
				}
				zmRes := zms[args[0].AuxId]
				for i := 1; i < len(args); i++ {
					if res, ok = zmRes.Or(zms[args[i].AuxId]); !ok {
						zmRes.Reset()
						break
					} else {
						zmRes = index.SetBool(zmRes, res)
					}
				}
				zms[expr.AuxId] = zmRes

			case "+":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMPlus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			case "-":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMMinus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			case "*":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMMulti(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			default:
				ivecs := make([]*vector.Vector, len(args))
				if isAllConst(args) { // constant fold
					for i, arg := range args {
						if vecs[arg.AuxId] != nil {
							vecs[arg.AuxId].Free(proc.Mp())
						}
						if vecs[arg.AuxId], _, err = GetReadonlyResultFromNoColumnExpression(proc, arg); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
						if ivecs[i], err = vecs[arg.AuxId].Dup(proc.Mp()); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
					}
				} else {
					if f() {
						return zms[expr.AuxId]
					}
					for i, arg := range args {
						if vecs[arg.AuxId] != nil {
							vecs[arg.AuxId].Free(proc.Mp())
						}
						if vecs[arg.AuxId], err = index.ZMToVector(zms[arg.AuxId], vecs[arg.AuxId], proc.Mp()); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
						ivecs[i] = vecs[arg.AuxId]
					}
				}
				fn, _, fnFree := overload.GetExecuteMethod()
				typ := types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)

				result := vector.NewFunctionResultWrapper(typ, proc.Mp())
				if err = result.PreExtendAndReset(2); err != nil {
					zms[expr.AuxId].Reset()
					result.Free()
					if fnFree != nil {
						// NOTE: fnFree is only applicable for serial and serial_full.
						// if fnFree is not nil, then make sure to call it after fn() is done.
						_ = fnFree()
					}
					return zms[expr.AuxId]
				}
				if err = fn(ivecs, result, proc, 2, nil); err != nil {
					zms[expr.AuxId].Reset()
					result.Free()
					if fnFree != nil {
						// NOTE: fnFree is only applicable for serial and serial_full.
						// if fnFree is not nil, then make sure to call it after fn() is done.
						_ = fnFree()
					}
					return zms[expr.AuxId]
				}
				if fnFree != nil {
					// NOTE: fnFree is only applicable for serial and serial_full.
					// if fnFree is not nil, then make sure to call it after fn() is done.
					_ = fnFree()
				}
				zms[expr.AuxId] = index.VectorToZM(result.GetResultVector(), zms[expr.AuxId])
				result.Free()
			}
		}

	default:
		zms[expr.AuxId].Reset()
	}

	return zms[expr.AuxId]
}

// RewriteFilterExprList will convert an expression list to be an AndExpr
func RewriteFilterExprList(list []*plan.Expr) *plan.Expr {
	l := len(list)
	if l == 0 {
		return nil
	} else if l == 1 {
		return list[0]
	} else {
		left := list[0]
		right := RewriteFilterExprList(list[1:])
		return &plan.Expr{
			Typ:  left.Typ,
			Expr: makeAndExpr(left, right),
		}
	}
}

func SplitAndExprs(list []*plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, len(list))
	for i := range list {
		exprs = append(exprs, splitAndExpr(list[i])...)
	}
	return exprs
}

func splitAndExpr(expr *plan.Expr) []*plan.Expr {
	if expr == nil {
		return nil
	}
	exprs := make([]*plan.Expr, 0, 1)
	if e, ok := expr.Expr.(*plan.Expr_F); ok {
		fid, _ := function.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function.AND {
			exprs = append(exprs, splitAndExpr(e.F.Args[0])...)
			exprs = append(exprs, splitAndExpr(e.F.Args[1])...)
			return exprs
		}
	}
	exprs = append(exprs, expr)
	return exprs
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr_F {
	return &plan.Expr_F{
		F: &plan.Function{
			Func: &plan.ObjectRef{
				Obj:     function.AndFunctionEncodedID,
				ObjName: function.AndFunctionName,
			},
			Args: []*plan.Expr{left, right},
		},
	}
}

func isAllConst(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if !isConst(expr) {
			return false
		}
	}
	return true
}

func isConst(expr *plan.Expr) bool {
	switch t := expr.Expr.(type) {
	case *plan.Expr_Col:
		return false
	case *plan.Expr_F:
		return isAllConst(t.F.Args)
	default:
		return true
	}
}
