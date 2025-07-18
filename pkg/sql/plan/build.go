// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	gotrace "runtime/trace"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func bindAndOptimizeSelectQuery(stmtType plan.Query_StatementType, ctx CompilerContext, stmt *tree.Select, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildSelectHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(stmtType, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindSelect(stmt, bindCtx, true)
	if err != nil {
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeInsertQuery(ctx CompilerContext, stmt *tree.Insert, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	builder.parseOptimizeHints()
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindInsert(stmt, bindCtx)
	if err != nil {
		if err.(*moerr.Error).ErrorCode() == moerr.ErrUnsupportedDML {
			return buildInsert(stmt, ctx, false, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeReplaceQuery(ctx CompilerContext, stmt *tree.Replace, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	builder.parseOptimizeHints()
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindReplace(stmt, bindCtx)
	if err != nil {
		if err.(*moerr.Error).ErrorCode() == moerr.ErrUnsupportedDML {
			return buildReplace(stmt, ctx, false, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeLoadQuery(ctx CompilerContext, stmt *tree.Load, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	// return buildLoad(stmt, ctx, isPrepareStmt)
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_INSERT, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindLoad(stmt, bindCtx)
	if err != nil {
		if err.(*moerr.Error).ErrorCode() == moerr.ErrUnsupportedDML {
			return buildLoad(stmt, ctx, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeDeleteQuery(ctx CompilerContext, stmt *tree.Delete, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildDeleteHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_DELETE, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindDelete(ctx, stmt, bindCtx)
	if err != nil {
		if err.(*moerr.Error).ErrorCode() == moerr.ErrUnsupportedDML {
			return buildDelete(stmt, ctx, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func bindAndOptimizeUpdateQuery(ctx CompilerContext, stmt *tree.Update, isPrepareStmt bool, skipStats bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildDeleteHistogram.Observe(time.Since(start).Seconds())
	}()

	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, isPrepareStmt, true)
	bindCtx := NewBindContext(builder, nil)
	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	rootId, err := builder.bindUpdate(stmt, bindCtx)
	if err != nil {
		if err.(*moerr.Error).ErrorCode() == moerr.ErrUnsupportedDML {
			return buildTableUpdate(stmt, ctx, isPrepareStmt)
		}
		return nil, err
	}
	ctx.SetViews(bindCtx.views)

	builder.qry.Steps = append(builder.qry.Steps, rootId)
	builder.skipStats = skipStats
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func buildExplainPlan(ctx CompilerContext, stmt tree.Statement, isPrepareStmt bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildExplainHistogram.Observe(time.Since(start).Seconds())
	}()

	//get query optimizer and execute Optimize
	plan, err := BuildPlan(ctx, stmt, isPrepareStmt)
	if err != nil {
		return nil, err
	}

	//if it is the plan of the EXECUTE, replace it by the plan generated by the PREPARE.
	//At the same time, replace the param var by the param val
	if plan.GetDcl() != nil && plan.GetDcl().GetExecute() != nil {
		execPlan := plan.GetDcl().GetExecute()
		replaced, _, err := ctx.InitExecuteStmtParam(execPlan)
		if err != nil {
			return nil, err
		}
		plan = replaced
	}

	// Ensure that the plan includes a query section
	if plan.GetQuery() == nil {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "the sql query plan does not support explain.")
	}

	return plan, nil
}

func buildExplainAnalyze(ctx CompilerContext, stmt *tree.ExplainAnalyze, isPrepareStmt bool) (*Plan, error) {
	return buildExplainPlan(ctx, stmt.Statement, isPrepareStmt)
}

func buildExplainPhyPlan(ctx CompilerContext, stmt *tree.ExplainPhyPlan, isPrepareStmt bool) (*Plan, error) {
	return buildExplainPlan(ctx, stmt.Statement, isPrepareStmt)
}

func BuildPlan(ctx CompilerContext, stmt tree.Statement, isPrepareStmt bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildPlanHistogram.Observe(time.Since(start).Seconds())
	}()
	_, task := gotrace.NewTask(context.TODO(), "plan.BuildPlan")
	defer task.End()
	switch stmt := stmt.(type) {
	case *tree.Select:
		return bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt, isPrepareStmt, false)
	case *tree.ParenSelect:
		return bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt.Select, isPrepareStmt, false)
	case *tree.ExplainAnalyze:
		return buildExplainAnalyze(ctx, stmt, isPrepareStmt)
	case *tree.ExplainPhyPlan:
		return buildExplainPhyPlan(ctx, stmt, isPrepareStmt)
	case *tree.Insert:
		return bindAndOptimizeInsertQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.Replace:
		return bindAndOptimizeReplaceQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.Update:
		return bindAndOptimizeUpdateQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.Delete:
		return bindAndOptimizeDeleteQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.BeginTransaction:
		return buildBeginTransaction(stmt, ctx)
	case *tree.CommitTransaction:
		return buildCommitTransaction(stmt, ctx)
	case *tree.RollbackTransaction:
		return buildRollbackTransaction(stmt, ctx)
	case *tree.CreateDatabase:
		return buildCreateDatabase(stmt, ctx)
	case *tree.DropDatabase:
		return buildDropDatabase(stmt, ctx)
	case *tree.CreateTable:
		return buildCreateTable(stmt, ctx)
	case *tree.CreatePitr:
		return buildCreatePitr(stmt, ctx)
	case *tree.DropPitr:
		return buildDropPitr(stmt, ctx)
	case *tree.DropTable:
		return buildDropTable(stmt, ctx)
	case *tree.TruncateTable:
		return buildTruncateTable(stmt, ctx)
	case *tree.CreateSequence:
		return buildCreateSequence(stmt, ctx)
	case *tree.DropSequence:
		return buildDropSequence(stmt, ctx)
	case *tree.AlterSequence:
		return buildAlterSequence(stmt, ctx)
	case *tree.DropView:
		return buildDropView(stmt, ctx)
	case *tree.CreateView:
		return buildCreateView(stmt, ctx)
	case *tree.CreateSource:
		return buildCreateSource(stmt, ctx)
	case *tree.AlterView:
		return buildAlterView(stmt, ctx)
	case *tree.AlterTable:
		return buildAlterTable(stmt, ctx)
	case *tree.RenameTable:
		return buildRenameTable(stmt, ctx)
	case *tree.CreateIndex:
		return buildCreateIndex(stmt, ctx)
	case *tree.DropIndex:
		return buildDropIndex(stmt, ctx)
	case *tree.ShowCreateDatabase:
		return buildShowCreateDatabase(stmt, ctx)
	case *tree.ShowCreateTable:
		return buildShowCreateTable(stmt, ctx)
	case *tree.ShowCreateView:
		return buildShowCreateView(stmt, ctx)
	case *tree.ShowDatabases:
		return buildShowDatabases(stmt, ctx)
	case *tree.ShowTables:
		return buildShowTables(stmt, ctx)
	case *tree.ShowSequences:
		return buildShowSequences(stmt, ctx)
	case *tree.ShowColumns:
		return buildShowColumns(stmt, ctx)
	case *tree.ShowTableStatus:
		return buildShowTableStatus(stmt, ctx)
	case *tree.ShowTarget:
		return buildShowTarget(stmt, ctx)
	case *tree.ShowIndex:
		return buildShowIndex(stmt, ctx)
	case *tree.ShowGrants:
		return buildShowGrants(stmt, ctx)
	case *tree.ShowVariables:
		return buildShowVariables(stmt, ctx)
	case *tree.ShowStatus:
		return buildShowStatus(stmt, ctx)
	case *tree.ShowProcessList:
		return buildShowProcessList(ctx)
	case *tree.ShowLocks:
		return buildShowLocks(stmt, ctx)
	case *tree.ShowNodeList:
		return buildShowNodeList(stmt, ctx)
	case *tree.ShowFunctionOrProcedureStatus:
		return buildShowFunctionOrProcedureStatus(stmt, ctx)
	case *tree.ShowTableNumber:
		return buildShowTableNumber(stmt, ctx)
	case *tree.ShowColumnNumber:
		return buildShowColumnNumber(stmt, ctx)
	case *tree.ShowTableValues:
		return buildShowTableValues(stmt, ctx)
	case *tree.ShowRolesStmt:
		return buildShowRoles(stmt, ctx)
	case *tree.SetVar:
		return buildSetVariables(stmt, ctx)
	case *tree.Execute:
		return buildExecute(stmt, ctx)
	case *tree.Deallocate:
		return buildDeallocate(stmt, ctx)
	case *tree.Load:
		return bindAndOptimizeLoadQuery(ctx, stmt, isPrepareStmt, false)
	case *tree.PrepareStmt, *tree.PrepareString:
		return buildPrepare(stmt, ctx)
	case *tree.Do, *tree.Declare:
		return nil, moerr.NewNotSupported(ctx.GetContext(), tree.String(stmt, dialect.MYSQL))
	case *tree.ValuesStatement:
		return buildValues(stmt, ctx, isPrepareStmt)
	case *tree.LockTableStmt:
		return buildLockTables(stmt, ctx)
	case *tree.UnLockTableStmt:
		return buildUnLockTables(stmt, ctx)
	case *tree.ShowCreatePublications:
		return buildShowCreatePublications(stmt, ctx)
	case *tree.ShowStages:
		return buildShowStages(stmt, ctx)
	case *tree.ShowSnapShots:
		return buildShowSnapShots(stmt, ctx)
	case *tree.CreateAccount:
		return buildCreateAccount(stmt, ctx, isPrepareStmt)
	case *tree.AlterAccount:
		return buildAlterAccount(stmt, ctx, isPrepareStmt)
	case *tree.DropAccount:
		return buildDropAccount(stmt, ctx, isPrepareStmt)
	case *tree.ShowAccountUpgrade:
		return buildShowAccountUpgrade(stmt, ctx)
	case *tree.ShowPitr:
		return buildShowPitr(stmt, ctx)
	case *tree.CloneTable:
		return buildCloneTable(stmt, ctx)
	default:
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}
}

// GetResultColumnsFromPlan
func GetResultColumnsFromPlan(p *Plan) []*ColDef {
	getResultColumnsByProjectionlist := func(query *Query) []*ColDef {
		lastNode := query.Nodes[query.Steps[len(query.Steps)-1]]
		columns := make([]*ColDef, len(lastNode.ProjectList))
		for idx, expr := range lastNode.ProjectList {
			columns[idx] = &ColDef{
				Name: query.Headings[idx],
				Typ:  expr.Typ,
			}

			if exprCol, ok := expr.Expr.(*plan.Expr_Col); ok {
				if col := exprCol.Col; col != nil {
					columns[idx].TblName = col.TblName
					columns[idx].DbName = col.DbName
				}
			}
		}

		return columns
	}

	switch logicPlan := p.Plan.(type) {
	case *plan.Plan_Query:
		switch logicPlan.Query.StmtType {
		case plan.Query_SELECT:
			return getResultColumnsByProjectionlist(logicPlan.Query)
		default:
			// insert/update/delete statement will return nil
			return nil
		}
	case *plan.Plan_Tcl:
		// begin/commmit/rollback statement will return nil
		return nil
	case *plan.Plan_Ddl:
		switch logicPlan.Ddl.DdlType {
		case plan.DataDefinition_SHOW_VARIABLES:
			typ := plan.Type{
				Id:    int32(types.T_varchar),
				Width: 1024,
			}
			return []*ColDef{
				{Typ: typ, Name: "Variable_name"},
				{Typ: typ, Name: "Value"},
			}
		default:
			// show statement(except show variables) will return a query
			if logicPlan.Ddl.Query != nil {
				return getResultColumnsByProjectionlist(logicPlan.Ddl.Query)
			}
			return nil
		}
	}
	return nil
}
