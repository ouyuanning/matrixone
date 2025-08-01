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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
)

func genDynamicTableDef(ctx CompilerContext, stmt *tree.Select) (*plan.TableDef, error) {
	var tableDef plan.TableDef

	// check view statement
	var stmtPlan *Plan
	var err error
	switch s := stmt.Select.(type) {
	case *tree.ParenSelect:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, s.Select, false, true)
		if err != nil {
			return nil, err
		}
	default:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt, false, true)
		if err != nil {
			return nil, err
		}
	}

	query := stmtPlan.GetQuery()
	cols := make([]*plan.ColDef, len(query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList))
	for idx, expr := range query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList {
		cols[idx] = &plan.ColDef{
			Name: strings.ToLower(query.Headings[idx]),
			Alg:  plan.CompressType_Lz4,
			Typ:  expr.Typ,
			Default: &plan.Default{
				NullAbility:  !expr.Typ.NotNullable,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	tableDef.Cols = cols

	viewData, err := json.Marshal(ViewData{
		Stmt:            ctx.GetRootSql(),
		DefaultDatabase: ctx.DefaultDatabase(),
	})
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql = &plan.ViewDef{
		View: string(viewData),
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

	return &tableDef, nil
}

func genViewTableDef(ctx CompilerContext, stmt *tree.Select) (*plan.TableDef, error) {
	var tableDef plan.TableDef

	// check view statement
	var stmtPlan *Plan
	var err error
	switch s := stmt.Select.(type) {
	case *tree.ParenSelect:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, s.Select, false, true)
		if err != nil {
			return nil, err
		}
	default:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt, false, true)
		if err != nil {
			return nil, err
		}
	}

	query := stmtPlan.GetQuery()
	cols := make([]*plan.ColDef, len(query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList))
	for idx, expr := range query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList {
		cols[idx] = &plan.ColDef{
			Name: strings.ToLower(query.Headings[idx]),
			Alg:  plan.CompressType_Lz4,
			Typ:  expr.Typ,
			Default: &plan.Default{
				NullAbility:  !expr.Typ.NotNullable,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	tableDef.Cols = cols

	// Check alter and change the viewsql.
	viewSql := ctx.GetRootSql()
	// remove sql hint
	viewSql = cleanHint(viewSql)
	if len(viewSql) != 0 {
		if viewSql[0] == 'A' {
			viewSql = strings.Replace(viewSql, "ALTER", "CREATE", 1)
		}
		if viewSql[0] == 'a' {
			viewSql = strings.Replace(viewSql, "alter", "create", 1)
		}
	}

	viewData, err := json.Marshal(ViewData{
		Stmt:            viewSql,
		DefaultDatabase: ctx.DefaultDatabase(),
	})
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql = &plan.ViewDef{
		View: string(viewData),
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemViewRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

	return &tableDef, nil
}

func genAsSelectCols(ctx CompilerContext, stmt *tree.Select) ([]*ColDef, error) {
	var err error
	var rootId int32
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)

	getTblAndColName := func(relPos, colPos int32) (string, string) {
		name := builder.nameByColRef[[2]int32{relPos, colPos}]
		// name pattern: tableName.colName
		splits := strings.Split(name, ".")
		if len(splits) < 2 {
			return "", ""
		}
		return splits[0], splits[1]
	}

	if s, ok := stmt.Select.(*tree.ParenSelect); ok {
		stmt = s.Select
	}
	if rootId, err = builder.bindSelect(stmt, bindCtx, true); err != nil {
		return nil, err
	}
	rootNode := builder.qry.Nodes[rootId]

	cols := make([]*plan.ColDef, len(rootNode.ProjectList))
	for i, expr := range rootNode.ProjectList {
		defaultVal := ""
		typ := &expr.Typ
		switch e := expr.Expr.(type) {
		case *plan.Expr_Col:
			tblName, colName := getTblAndColName(e.Col.RelPos, e.Col.ColPos)
			if binding, ok := bindCtx.bindingByTable[tblName]; ok {
				defaultVal = binding.defaults[binding.colIdByName[colName]]
			}
		case *plan.Expr_F:
			// enum
			if e.F.Func.ObjName == moEnumCastIndexToValueFun {
				// cast_index_to_value('apple,banana,orange', cast(col_name as T_uint16))
				colRef := e.F.Args[1].Expr.(*plan.Expr_Col).Col
				tblName, colName := getTblAndColName(colRef.RelPos, colRef.ColPos)
				if binding, ok := bindCtx.bindingByTable[tblName]; ok {
					typ = binding.types[binding.colIdByName[colName]]
				}
			}
		}

		cols[i] = &plan.ColDef{
			Name: strings.ToLower(bindCtx.headings[i]),
			Alg:  plan.CompressType_Lz4,
			Typ:  *typ,
			Default: &plan.Default{
				NullAbility:  !expr.Typ.NotNullable,
				Expr:         nil,
				OriginString: defaultVal,
			},
		}
	}
	return cols, nil
}

func buildCreateSource(stmt *tree.CreateSource, ctx CompilerContext) (*Plan, error) {
	streamName := string(stmt.SourceName.ObjectName)
	createStream := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			TableType: catalog.SystemSourceRel,
			Name:      streamName,
		},
	}
	if len(stmt.SourceName.SchemaName) == 0 {
		createStream.Database = ctx.DefaultDatabase()
	} else {
		createStream.Database = string(stmt.SourceName.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(createStream.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create stream in subscription database")
	}

	if err := buildSourceDefs(stmt, ctx, createStream); err != nil {
		return nil, err
	}

	var properties []*plan.Property
	properties = append(properties, &plan.Property{
		Key:   catalog.SystemRelAttr_Kind,
		Value: catalog.SystemSourceRel,
	})
	configs := make(map[string]interface{})
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.CreateSourceWithOption:
			key := strings.ToLower(string(opt.Key))
			val := opt.Val.(*tree.NumVal).String()
			properties = append(properties, &plan.Property{
				Key:   key,
				Value: val,
			})
			configs[key] = val
		}
	}
	if err := mokafka.ValidateConfig(context.Background(), configs, mokafka.NewKafkaAdapter); err != nil {
		return nil, err
	}
	createStream.TableDef.Defs = append(createStream.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createStream,
				},
			},
		},
	}, nil
}

func buildSourceDefs(stmt *tree.CreateSource, ctx CompilerContext, createStream *plan.CreateTable) error {
	colMap := make(map[string]*ColDef)
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colName := def.Name.ColName()
			colNameOrigin := def.Name.ColNameOrigin()
			if _, ok := colMap[colName]; ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate column name: %s", colNameOrigin)
			}
			colType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
				colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInputf(ctx.GetContext(), "string width (%d) is too long", colType.GetWidth())
				}
			}
			col := &ColDef{
				Name:       colName,
				OriginName: colNameOrigin,
				Alg:        plan.CompressType_Lz4,
				Typ:        colType,
			}
			colMap[colName] = col
			for _, attr := range def.Attributes {
				switch a := attr.(type) {
				case *tree.AttributeKey:
					col.Primary = true
				case *tree.AttributeHeader:
					col.Header = a.Key
				case *tree.AttributeHeaders:
					col.Headers = true
				}
			}
			createStream.TableDef.Cols = append(createStream.TableDef.Cols, col)
		case *tree.CreateSourceWithOption:
		default:
			return moerr.NewNYIf(ctx.GetContext(), "stream def: '%v'", def)
		}
	}
	return nil
}

func buildCreateView(stmt *tree.CreateView, ctx CompilerContext) (*Plan, error) {
	viewName := stmt.Name.ObjectName

	createView := &plan.CreateView{
		Replace:     stmt.Replace,
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			Name: string(viewName),
		},
	}

	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		createView.Database = ""
	} else {
		createView.Database = string(stmt.Name.SchemaName)
	}
	if len(createView.Database) == 0 {
		createView.Database = ctx.DefaultDatabase()
	}

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if IsSnapshotValid(ctx.GetSnapshot()) {
		snapshot = ctx.GetSnapshot()
	}

	if sub, err := ctx.GetSubscriptionMeta(createView.Database, snapshot); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create view in subscription database")
	}

	tableDef, err := genViewTableDef(ctx, stmt.AsSource)
	if err != nil {
		return nil, err
	}

	createView.TableDef.Cols = tableDef.Cols
	createView.TableDef.ViewSql = tableDef.ViewSql
	createView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_VIEW,
				Definition: &plan.DataDefinition_CreateView{
					CreateView: createView,
				},
			},
		},
	}, nil
}

func buildSequenceTableDef(stmt *tree.CreateSequence, ctx CompilerContext, cs *plan.CreateSequence) error {
	// Sequence table got 1 row and 7 col
	// sequence_value, maxvalue,minvalue,startvalue,increment,cycleornot,iscalled.
	cols := make([]*plan.ColDef, len(Sequence_cols_name))

	typ, err := getTypeFromAst(ctx.GetContext(), stmt.Type)
	if err != nil {
		return err
	}
	for i := range cols {
		if i == 4 {
			break
		}
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ:  typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	cols[4] = &plan.ColDef{
		Name: Sequence_cols_name[4],
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int64),
			Width: 0,
			Scale: 0,
		},
		Primary: true,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	cs.TableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{Sequence_cols_name[4]},
		PkeyColName: Sequence_cols_name[4],
	}
	for i := 5; i <= 6; i++ {
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_bool),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}

	cs.TableDef.Cols = cols

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemSequenceRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}

	cs.TableDef.Defs = append(cs.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return nil
}

func buildAlterSequenceTableDef(stmt *tree.AlterSequence, ctx CompilerContext, as *plan.AlterSequence) error {
	// Sequence table got 1 row and 7 col
	// sequence_value, maxvalue,minvalue,startvalue,increment,cycleornot,iscalled.
	cols := make([]*plan.ColDef, len(Sequence_cols_name))

	var typ plan.Type
	var err error
	if stmt.Type == nil {
		_, tableDef, err := ctx.Resolve(as.GetDatabase(), as.TableDef.Name, nil)
		if err != nil {
			return err
		}
		if tableDef == nil {
			return moerr.NewInvalidInputf(ctx.GetContext(), "no such sequence %s", as.TableDef.Name)
		} else {
			typ = tableDef.Cols[0].Typ
		}
	} else {
		typ, err = getTypeFromAst(ctx.GetContext(), stmt.Type.Type)
		if err != nil {
			return err
		}
	}

	for i := range cols {
		if i == 4 {
			break
		}
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ:  typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	cols[4] = &plan.ColDef{
		Name: Sequence_cols_name[4],
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int64),
			Width: 0,
			Scale: 0,
		},
		Primary: true,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	as.TableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{Sequence_cols_name[4]},
		PkeyColName: Sequence_cols_name[4],
	}
	for i := 5; i <= 6; i++ {
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_bool),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}

	as.TableDef.Cols = cols

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemSequenceRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}

	as.TableDef.Defs = append(as.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return nil

}

func buildDropSequence(stmt *tree.DropSequence, ctx CompilerContext) (*Plan, error) {
	dropSequence := &plan.DropSequence{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) Sequence in one statement", len(stmt.Names))
	}
	dropSequence.Database = string(stmt.Names[0].SchemaName)
	if dropSequence.Database == "" {
		dropSequence.Database = ctx.DefaultDatabase()
	}
	dropSequence.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropSequence.Database, dropSequence.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil || tableDef.TableType != catalog.SystemSequenceRel {
		if !dropSequence.IfExists {
			return nil, moerr.NewNoSuchSequence(ctx.GetContext(), dropSequence.Database, dropSequence.Table)
		}
		dropSequence.Table = ""
	}
	if obj != nil && obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop sequence in subscription database")
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_SEQUENCE,
				Definition: &plan.DataDefinition_DropSequence{
					DropSequence: dropSequence,
				},
			},
		},
	}, nil
}

func buildAlterSequence(stmt *tree.AlterSequence, ctx CompilerContext) (*Plan, error) {
	if stmt.Type == nil && stmt.IncrementBy == nil && stmt.MaxValue == nil && stmt.MinValue == nil && stmt.StartWith == nil && stmt.Cycle == nil {
		return nil, moerr.NewSyntaxErrorf(ctx.GetContext(), "synatx error, %s has nothing to alter", string(stmt.Name.ObjectName))
	}

	alterSequence := &plan.AlterSequence{
		IfExists: stmt.IfExists,
		TableDef: &TableDef{
			Name: string(stmt.Name.ObjectName),
		},
	}
	// Get database name.
	if len(stmt.Name.SchemaName) == 0 {
		alterSequence.Database = ctx.DefaultDatabase()
	} else {
		alterSequence.Database = string(stmt.Name.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(alterSequence.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter sequence in subscription database")
	}

	err := buildAlterSequenceTableDef(stmt, ctx, alterSequence)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_SEQUENCE,
				Definition: &plan.DataDefinition_AlterSequence{
					AlterSequence: alterSequence,
				},
			},
		},
	}, nil
}

func buildCreateSequence(stmt *tree.CreateSequence, ctx CompilerContext) (*Plan, error) {
	createSequence := &plan.CreateSequence{
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			Name: string(stmt.Name.ObjectName),
		},
	}
	// Get database name.
	if len(stmt.Name.SchemaName) == 0 {
		createSequence.Database = ctx.DefaultDatabase()
	} else {
		createSequence.Database = string(stmt.Name.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(createSequence.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create sequence in subscription database")
	}

	err := buildSequenceTableDef(stmt, ctx, createSequence)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_SEQUENCE,
				Definition: &plan.DataDefinition_CreateSequence{
					CreateSequence: createSequence,
				},
			},
		},
	}, nil
}

func buildCreateTable(
	stmt *tree.CreateTable,
	ctx CompilerContext,
) (*Plan, error) {

	if stmt.IsAsLike {
		var err error
		oldTable := stmt.LikeTableName
		newTable := stmt.Table
		tblName := formatStr(string(oldTable.ObjectName))
		dbName := formatStr(string(oldTable.SchemaName))

		var snapshot *Snapshot
		snapshot = ctx.GetSnapshot()
		if snapshot == nil {
			snapshot = &Snapshot{TS: &timestamp.Timestamp{}}
		}

		if dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, snapshot); err != nil {
			return nil, err
		}

		// check if the database is a subscription
		sub, err := ctx.GetSubscriptionMeta(dbName, snapshot)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			ctx.SetQueryingSubscription(sub)
			defer func() {
				ctx.SetQueryingSubscription(nil)
			}()
		}

		_, tableDef, err := ctx.Resolve(dbName, tblName, snapshot)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
		}
		// TODO WHY?
		if tableDef.TableType == catalog.SystemViewRel || tableDef.TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "%s.%s is not BASE TABLE", dbName, tblName)
		}

		tableDef.Name = string(newTable.ObjectName)
		tableDef.DbName = string(newTable.SchemaName)
		if len(tableDef.DbName) == 0 {
			tableDef.DbName = ctx.DefaultDatabase()
		}

		_, newStmt, err := ConstructCreateTableSQL(ctx, tableDef, snapshot, true)
		if err != nil {
			return nil, err
		}
		if stmtLike, ok := newStmt.(*tree.CreateTable); ok {
			return buildCreateTable(stmtLike, ctx)
		}

		return nil, moerr.NewInternalError(ctx.GetContext(), "rewrite for create table like failed")
	}

	createTable := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		Temporary:   stmt.Temporary,
		TableDef: &TableDef{
			Name: string(stmt.Table.ObjectName),
		},
	}

	if stmt.PartitionOption != nil {
		createTable.RawSQL = tree.StringWithOpts(
			stmt,
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithSingleQuoteString(),
		)
		createTable.TableDef.FeatureFlag |= features.Partitioned
	}

	// get database name
	if len(stmt.Table.SchemaName) == 0 {
		createTable.Database = ctx.DefaultDatabase()
	} else {
		createTable.Database = string(stmt.Table.SchemaName)
	}

	if stmt.Temporary && stmt.PartitionOption != nil {
		return nil, moerr.NewPartitionNoTemporary(ctx.GetContext())
	}

	if sub, err := ctx.GetSubscriptionMeta(createTable.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create table in subscription database")
	}

	// set tableDef
	var err error
	if stmt.IsDynamicTable {
		tableDef, err := genDynamicTableDef(ctx, stmt.AsSource)
		if err != nil {
			return nil, err
		}

		createTable.TableDef.Cols = tableDef.Cols
		//createTable.TableDef.ViewSql = tableDef.ViewSql
		//createTable.TableDef.Defs = tableDef.Defs
	}

	var asSelectCols []*ColDef
	if stmt.IsAsSelect {
		if asSelectCols, err = genAsSelectCols(ctx, stmt.AsSource); err != nil {
			return nil, err
		}
	}

	if err = buildTableDefs(stmt, ctx, createTable, asSelectCols); err != nil {
		return nil, err
	}

	v, ok := getAutoIncrementOffsetFromVariables(ctx)
	if ok {
		createTable.TableDef.AutoIncrOffset = v
	}

	// set option
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.TableOptionProperties:
			properties := make([]*plan.Property, len(opt.Preperties))
			for idx, property := range opt.Preperties {
				properties[idx] = &plan.Property{
					Key:   property.Key,
					Value: property.Value,
				}
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		// todo confirm: option data store like this?
		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "comment for field '%s' is too long", createTable.TableDef.Name)
			}

			properties := []*plan.Property{
				{
					Key:   catalog.SystemRelAttr_Comment,
					Value: opt.Comment,
				},
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		case *tree.TableOptionAutoIncrement:
			if opt.Value != 0 {
				createTable.TableDef.AutoIncrOffset = opt.Value - 1
			}

		// these table options is not support in plan
		// case *tree.TableOptionEngine, *tree.TableOptionSecondaryEngine, *tree.TableOptionCharset,
		// 	*tree.TableOptionCollate, *tree.TableOptionAutoIncrement, *tree.TableOptionComment,
		// 	*tree.TableOptionAvgRowLength, *tree.TableOptionChecksum, *tree.TableOptionCompression,
		// 	*tree.TableOptionConnection, *tree.TableOptionPassword, *tree.TableOptionKeyBlockSize,
		// 	*tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionDelayKeyWrite,
		// 	*tree.TableOptionRowFormat, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsAutoRecalc,
		// 	*tree.TableOptionPackKeys, *tree.TableOptionTablespace, *tree.TableOptionDataDirectory,
		// 	*tree.TableOptionIndexDirectory, *tree.TableOptionStorageMedia, *tree.TableOptionStatsSamplePages,
		// 	*tree.TableOptionUnion, *tree.TableOptionEncryption:
		// 	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
		case *tree.TableOptionAUTOEXTEND_SIZE, *tree.TableOptionAvgRowLength,
			*tree.TableOptionCharset, *tree.TableOptionChecksum, *tree.TableOptionCollate, *tree.TableOptionCompression,
			*tree.TableOptionConnection, *tree.TableOptionDataDirectory, *tree.TableOptionIndexDirectory,
			*tree.TableOptionDelayKeyWrite, *tree.TableOptionEncryption, *tree.TableOptionEngine, *tree.TableOptionEngineAttr,
			*tree.TableOptionKeyBlockSize, *tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionPackKeys,
			*tree.TableOptionPassword, *tree.TableOptionRowFormat, *tree.TableOptionStartTrans, *tree.TableOptionSecondaryEngineAttr,
			*tree.TableOptionStatsAutoRecalc, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsSamplePages,
			*tree.TableOptionTablespace, *tree.TableOptionUnion:

		default:
			return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
		}
	}

	// After handleTableOptions, so begin the partitions processing depend on TableDef
	if stmt.Param != nil {
		for i := 0; i < len(stmt.Param.Option); i += 2 {
			switch strings.ToLower(stmt.Param.Option[i]) {
			case "endpoint", "region", "access_key_id", "secret_access_key", "bucket", "filepath", "compression", "format", "jsondata", "provider", "role_arn", "external_id":
			default:
				return nil, moerr.NewBadConfigf(ctx.GetContext(), "the keyword '%s' is not support", strings.ToLower(stmt.Param.Option[i]))
			}
		}
		if err := InitNullMap(stmt.Param, ctx); err != nil {
			return nil, err
		}
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return nil, err
		}
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemExternalRel,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: string(json_byte),
			},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	} else {
		kind := catalog.SystemOrdinaryRel
		if stmt.IsClusterTable {
			kind = catalog.SystemClusterRel
		}
		// when create hidden talbe(like: auto_incr_table, index_table)， we set relKind to empty
		if catalog.IsHiddenTable(createTable.TableDef.Name) {
			kind = ""
		}
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		stmt.Format(fmtCtx)
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: kind,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: ctx.GetRootSql(),
			},
		}
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindContext := NewBindContext(builder, nil)

	// set partition(unsupport now)
	if stmt.PartitionOption != nil {
		// Foreign keys are not yet supported in conjunction with partitioning
		// see: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-14.html
		if len(createTable.TableDef.Fkeys) > 0 {
			return nil, moerr.NewErrForeignKeyOnPartitioned(ctx.GetContext())
		}

		nodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       nil,
			ObjRef:      nil,
			TableDef:    createTable.TableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, bindContext)

		err = builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
		if err != nil {
			return nil, err
		}

		partitionBinder := NewPartitionBinder(builder, bindContext)
		createTable.TableDef.Partition, err = partitionBinder.buildPartitionDefs(ctx.GetContext(), stmt.PartitionOption)
		if err != nil {
			return nil, err
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createTable,
				},
			},
		},
	}, nil
}

func buildTableDefs(stmt *tree.CreateTable, ctx CompilerContext, createTable *plan.CreateTable, asSelectCols []*ColDef) error {
	// all below fields' key is lower case
	var primaryKeys []string
	var indexs []string
	var fulltext_indexs []string
	colMap := make(map[string]*ColDef)
	defaultMap := make(map[string]string)
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	fullTextIndexInfos := make([]*tree.FullTextIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	fkDatasOfFKSelfRefer := make([]*FkData, 0)
	dedupFkName := make(UnorderedSet[string])
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
				colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInputf(ctx.GetContext(), "string width (%d) is too long", colType.GetWidth())
				}
			}
			if colType.Id == int32(types.T_array_float32) || colType.Id == int32(types.T_array_float64) {
				if colType.GetWidth() > types.MaxArrayDimension {
					return moerr.NewInvalidInputf(ctx.GetContext(), "vector width (%d) is too long", colType.GetWidth())
				}
			}
			if colType.Id == int32(types.T_bit) {
				if colType.Width == 0 {
					colType.Width = 1
				}
				if colType.Width > types.MaxBitLen {
					return moerr.NewInvalidInputf(ctx.GetContext(), "bit width (%d) is too long (max = %d) ", colType.GetWidth(), types.MaxBitLen)
				}
			}
			var pks []string
			var comment string
			var auto_incr bool
			colName := def.Name.ColName()
			// only used in error message and ColDef.OriginName
			colNameOrigin := def.Name.ColNameOrigin()
			for _, attr := range def.Attributes {
				switch attribute := attr.(type) {
				case *tree.AttributePrimaryKey, *tree.AttributeKey:
					if colType.GetId() == int32(types.T_blob) {
						return moerr.NewNotSupported(ctx.GetContext(), "blob type in primary key")
					}
					if colType.GetId() == int32(types.T_text) {
						return moerr.NewNotSupported(ctx.GetContext(), "text type in primary key")
					}
					if colType.GetId() == int32(types.T_datalink) {
						return moerr.NewNotSupported(ctx.GetContext(), "datalink type in primary key")
					}
					if colType.GetId() == int32(types.T_json) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in primary key", colNameOrigin))
					}
					if colType.GetId() == int32(types.T_array_float32) || colType.GetId() == int32(types.T_array_float64) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("VECTOR column '%s' cannot be in primary key", colNameOrigin))
					}
					if colType.GetId() == int32(types.T_enum) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("ENUM column '%s' cannot be in primary key", colNameOrigin))

					}
					pks = append(pks, colName)
				case *tree.AttributeComment:
					comment = attribute.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						return moerr.NewInvalidInputf(ctx.GetContext(), "comment for column '%s' is too long", colNameOrigin)
					}
				case *tree.AttributeAutoIncrement:
					auto_incr = true
					if !types.T(colType.GetId()).IsInteger() {
						return moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
					}
				case *tree.AttributeUnique, *tree.AttributeUniqueKey:
					if colType.GetId() == int32(types.T_enum) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("ENUM column '%s' cannot be in unique index", colNameOrigin))

					}
					uniqueIndexInfos = append(uniqueIndexInfos, &tree.UniqueIndex{
						KeyParts: []*tree.KeyPart{{ColName: def.Name}},
						Name:     colName,
					})
					indexs = append(indexs, colName)
				}
			}
			if len(pks) > 0 {
				if len(primaryKeys) > 0 {
					return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
				}
				primaryKeys = pks
			}

			defaultValue, err := buildDefaultExpr(def, colType, ctx.GetProcess())
			if err != nil {
				return err
			}
			if auto_incr && defaultValue.Expr != nil {
				return moerr.NewInvalidInputf(ctx.GetContext(), "invalid default value for '%s'", colNameOrigin)
			}

			onUpdateExpr, err := buildOnUpdate(def, colType, ctx.GetProcess())
			if err != nil {
				return err
			}

			if !checkTableColumnNameValid(colName) {
				return moerr.NewInvalidInputf(ctx.GetContext(), "table column name '%s' is illegal and conflicts with internal keyword", colNameOrigin)
			}

			colType.AutoIncr = auto_incr
			col := &ColDef{
				Name:       colName,
				OriginName: colNameOrigin,
				Alg:        plan.CompressType_Lz4,
				Typ:        colType,
				Default:    defaultValue,
				OnUpdate:   onUpdateExpr,
				Comment:    comment,
			}
			// if same name col in asSelectCols, overwrite it; add into colMap && createTable.TableDef.Cols later
			if idx := slices.IndexFunc(asSelectCols, func(c *ColDef) bool { return c.Name == col.Name }); idx != -1 {
				asSelectCols[idx] = col
			} else {
				colMap[colName] = col
				createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)

				// get default val from ast node
				attrIdx := slices.IndexFunc(def.Attributes, func(a tree.ColumnAttribute) bool {
					_, ok := a.(*tree.AttributeDefault)
					return ok
				})
				if attrIdx != -1 {
					defaultAttr := def.Attributes[attrIdx].(*tree.AttributeDefault)
					fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
					defaultAttr.Format(fmtCtx)
					// defaultAttr.Format start with "default ", trim first 8 chars
					defaultMap[colName] = fmtCtx.String()[8:]
				} else {
					defaultMap[colName] = "NULL"
				}
			}
		case *tree.PrimaryKeyIndex:
			if len(primaryKeys) > 0 {
				return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
			}
			pksMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.ColName() // name of primary key column
				if _, ok := pksMap[name]; ok {
					return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate column name '%s' in primary key", key.ColName.ColNameOrigin())
				}

				if col, ok := colMap[name]; ok {
					if col.Typ.Id == int32(types.T_enum) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("ENUM column '%s' cannot be in primary key", name))
					}
				}

				primaryKeys = append(primaryKeys, name)
				pksMap[name] = true
				indexs = append(indexs, name)
			}
		case *tree.Index:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			secondaryIndexInfos = append(secondaryIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()

				if col, ok := colMap[name]; ok {
					if col.Typ.Id == int32(types.T_enum) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("ENUM column '%s' cannot be in secondary index", name))
					}
				}

				indexs = append(indexs, name)
			}
		case *tree.UniqueIndex:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			uniqueIndexInfos = append(uniqueIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()

				if col, ok := colMap[name]; ok {
					if col.Typ.Id == int32(types.T_enum) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("ENUM column '%s' cannot be in unique index", name))
					}
				}

				indexs = append(indexs, name)
			}
		case *tree.FullTextIndex:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			fullTextIndexInfos = append(fullTextIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()
				fulltext_indexs = append(fulltext_indexs, name)
			}
		case *tree.ForeignKey:
			if createTable.Temporary {
				return moerr.NewNYI(ctx.GetContext(), "add foreign key for temporary table")
			}
			if len(asSelectCols) != 0 {
				return moerr.NewNYI(ctx.GetContext(), "add foreign key in create table ... as select statement")
			}
			if IsFkBannedDatabase(createTable.Database) {
				return moerr.NewInternalErrorf(ctx.GetContext(), "can not create foreign keys in %s", createTable.Database)
			}
			err := adjustConstraintName(ctx.GetContext(), def)
			if err != nil {
				return err
			}
			fkData, err := getForeignKeyData(ctx, createTable.Database, createTable.TableDef, def)
			if err != nil {
				return err
			}

			if def.ConstraintSymbol != fkData.Def.Name {
				return moerr.NewInternalErrorf(ctx.GetContext(), "different fk name %s %s", def.ConstraintSymbol, fkData.Def.Name)
			}

			//dedup
			if dedupFkName.Find(fkData.Def.Name) {
				return moerr.NewInternalErrorf(ctx.GetContext(), "duplicate fk name %s", fkData.Def.Name)
			}
			dedupFkName.Insert(fkData.Def.Name)

			//only setups foreign key without forward reference
			if !fkData.ForwardRefer {
				createTable.FkDbs = append(createTable.FkDbs, fkData.ParentDbName)
				createTable.FkTables = append(createTable.FkTables, fkData.ParentTableName)
				createTable.FkCols = append(createTable.FkCols, fkData.Cols)
				createTable.TableDef.Fkeys = append(createTable.TableDef.Fkeys, fkData.Def)
			}

			createTable.UpdateFkSqls = append(createTable.UpdateFkSqls, fkData.UpdateSql)

			//save self reference foreign keys
			if fkData.IsSelfRefer {
				fkDatasOfFKSelfRefer = append(fkDatasOfFKSelfRefer, fkData)
			}
		case *tree.CheckIndex:
			// unsupport in plan. will support in next version.
			// return moerr.NewNYI(ctx.GetContext(), "table def: '%v'", def)
		default:
			return moerr.NewNYIf(ctx.GetContext(), "table def: '%v'", def)
		}
	}

	if stmt.IsAsSelect {
		// add as select cols
		for _, col := range asSelectCols {
			colMap[col.Name] = col
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)
		}

		// insert into new_table select default_val1, default_val2, ..., * from (select clause);
		var insertSqlBuilder strings.Builder
		insertSqlBuilder.WriteString(fmt.Sprintf("insert into `%s`.`%s` select ", createTable.Database, createTable.TableDef.Name))

		cols := createTable.TableDef.Cols
		firstCol := true
		for i := range cols {
			// insert default values if col[i] only in create clause
			if !slices.ContainsFunc(asSelectCols, func(c *ColDef) bool { return c.Name == cols[i].Name }) {
				if !firstCol {
					insertSqlBuilder.WriteString(", ")
				}
				insertSqlBuilder.WriteString(defaultMap[cols[i].Name])
				firstCol = false
			}
		}
		if !firstCol {
			insertSqlBuilder.WriteString(", ")
		}
		// add all cols from select clause
		insertSqlBuilder.WriteString("*")

		// from
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		stmt.AsSource.Format(fmtCtx)
		insertSqlBuilder.WriteString(fmt.Sprintf(" from (%s)", fmtCtx.String()))

		createTable.CreateAsSelectSql = insertSqlBuilder.String()
	}

	//table must have one visible column
	if len(createTable.TableDef.Cols) == 0 {
		return moerr.NewTableMustHaveVisibleColumn(ctx.GetContext())
	}

	//add cluster table attribute
	if stmt.IsClusterTable {
		if _, ok := colMap[util.GetClusterTableAttributeName()]; ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "the attribute account_id in the cluster table can not be defined directly by the user")
		}
		colType, err := getTypeFromAst(ctx.GetContext(), util.GetClusterTableAttributeType())
		if err != nil {
			return err
		}
		colDef := &ColDef{
			Name:    util.GetClusterTableAttributeName(),
			Alg:     plan.CompressType_Lz4,
			Typ:     colType,
			NotNull: true,
			Default: &plan.Default{
				Expr: &Expr{
					Expr: &plan.Expr_Lit{
						Lit: &Const{
							Isnull: false,
							Value:  &plan.Literal_U32Val{U32Val: catalog.System_Account},
						},
					},
					Typ: plan.Type{
						Id:          colType.Id,
						NotNullable: true,
					},
				},
				NullAbility: false,
			},
			Comment: "the account_id added by the mo",
		}
		colMap[util.GetClusterTableAttributeName()] = colDef
		createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
	}

	pkeyName := ""
	// If the primary key is explicitly defined in the ddl statement
	if len(primaryKeys) > 0 {
		for _, primaryKey := range primaryKeys {
			if _, ok := colMap[primaryKey]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' doesn't exist in table", primaryKey)
			}
		}
		if len(primaryKeys) == 1 {
			pkeyName = primaryKeys[0]
			for _, col := range createTable.TableDef.Cols {
				if col.Name == pkeyName {
					col.Primary = true
					createTable.TableDef.Pkey = &PrimaryKeyDef{
						Names:       primaryKeys,
						PkeyColName: pkeyName,
					}
					break
				}
			}
		} else {
			//pkeyName = util.BuildCompositePrimaryKeyColumnName(primaryKeys)
			pkeyName = catalog.CPrimaryKeyColName
			colDef := MakeHiddenColDefByName(pkeyName)
			colDef.Primary = true
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef

			pkeyDef := &PrimaryKeyDef{
				Names:       primaryKeys,
				PkeyColName: pkeyName,
				CompPkeyCol: colDef,
			}
			createTable.TableDef.Pkey = pkeyDef
		}
		for _, primaryKey := range primaryKeys {
			colMap[primaryKey].Default.NullAbility = false
			colMap[primaryKey].NotNull = true
		}
	} else {
		// If table does not have a explicit primary key in the ddl statement, a new hidden primary key column will be add,
		// which will not be sorted or used for any other purpose, but will only be used to add
		// locks to the Lock operator in pessimistic transaction mode.
		if !createTable.IsSystemExternalRel() {
			pkeyName = catalog.FakePrimaryKeyColName
			colDef := &ColDef{
				ColId:  uint64(len(createTable.TableDef.Cols)),
				Name:   pkeyName,
				Hidden: true,
				Typ: Type{
					Id:       int32(types.T_uint64),
					AutoIncr: true,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
				NotNull: true,
				Primary: true,
			}

			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef

			createTable.TableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{pkeyName},
				PkeyColName: pkeyName,
			}

			idx := len(createTable.TableDef.Cols) - 1
			// FIXME: due to the special treatment of insert and update for composite primary key, cluster-by, the
			// hidden primary key cannot be placed in the last column, otherwise it will cause the columns sent to
			// tae will not match the definition of schema, resulting in panic.
			if createTable.TableDef.ClusterBy != nil &&
				len(stmt.ClusterByOption.ColumnList) > 1 {
				// we must swap hide pk and cluster_by
				createTable.TableDef.Cols[idx-1], createTable.TableDef.Cols[idx] = createTable.TableDef.Cols[idx], createTable.TableDef.Cols[idx-1]
			}
		}
	}

	//handle cluster by keys
	if stmt.ClusterByOption != nil {
		if stmt.Temporary {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with temporary table is not support")
		}
		if len(primaryKeys) > 0 {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
		}
		lenClusterBy := len(stmt.ClusterByOption.ColumnList)
		var clusterByKeys []string
		for i := 0; i < lenClusterBy; i++ {
			colName := stmt.ClusterByOption.ColumnList[i].ColName()
			if _, ok := colMap[colName]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' doesn't exist in table", stmt.ClusterByOption.ColumnList[i].ColNameOrigin())
			}
			clusterByKeys = append(clusterByKeys, colName)
		}

		if lenClusterBy == 1 {
			clusterByColName := clusterByKeys[0]
			for _, col := range createTable.TableDef.Cols {
				if col.Name == clusterByColName {
					col.ClusterBy = true
				}
			}

			createTable.TableDef.ClusterBy = &plan.ClusterByDef{
				Name: clusterByColName,
			}
		} else {
			clusterByColName := util.BuildCompositeClusterByColumnName(clusterByKeys)
			colDef := MakeHiddenColDefByName(clusterByColName)
			colDef.Default.NullAbility = true
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[clusterByColName] = colDef

			createTable.TableDef.ClusterBy = &plan.ClusterByDef{
				Name:         clusterByColName,
				CompCbkeyCol: colDef,
			}
		}
	}

	// check index invalid on the type
	for _, str := range indexs {
		if _, ok := colMap[str]; !ok {
			return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", str)
		}
		if colMap[str].Typ.Id == int32(types.T_blob) {
			return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("BLOB column '%s' cannot be in index", str))
		}
		if colMap[str].Typ.Id == int32(types.T_text) {
			return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("TEXT column '%s' cannot be in index", str))
		}
		if colMap[str].Typ.Id == int32(types.T_datalink) {
			return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("DATALINK column '%s' cannot be in index", str))
		}
		if colMap[str].Typ.Id == int32(types.T_json) {
			return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in index", str))
		}
	}

	// check fulltext index invalid on the typ
	for _, str := range fulltext_indexs {
		if _, ok := colMap[str]; !ok {
			return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", str)
		}
		if colMap[str].Typ.Id == int32(types.T_blob) {
			return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("BLOB column '%s' cannot be in index", str))
		}
	}

	// check Constraint Name (include index/ unique)
	err := checkConstraintNames(uniqueIndexInfos, secondaryIndexInfos, ctx.GetContext())
	if err != nil {
		return err
	}

	// build index table
	if len(uniqueIndexInfos) != 0 {
		err = buildUniqueIndexTable(createTable, uniqueIndexInfos, colMap, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(fullTextIndexInfos) != 0 {
		err = buildFullTextIndexTable(createTable, fullTextIndexInfos, colMap, nil, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(secondaryIndexInfos) != 0 {
		err = buildSecondaryIndexDef(createTable, secondaryIndexInfos, colMap, nil, pkeyName, ctx)
		if err != nil {
			return err
		}
	}

	//process self reference foreign keys after colDefs and indexes are processed.
	if len(fkDatasOfFKSelfRefer) > 0 {
		//for fk self refer. the column id of the tableDef is not ready.
		//setup fake column id to distinguish the columns
		for i, def := range createTable.TableDef.Cols {
			def.ColId = uint64(i)
		}
		for _, selfRefer := range fkDatasOfFKSelfRefer {
			if err := checkFkColsAreValid(ctx, selfRefer, createTable.TableDef); err != nil {
				return err
			}
		}
	}

	skip := IsFkBannedDatabase(createTable.Database)
	if !skip {
		fks, err := GetFkReferredTo(ctx, createTable.Database, createTable.TableDef.Name)
		if err != nil {
			return err
		}
		//for fk forward reference. the column id of the tableDef is not ready.
		//setup fake column id to distinguish the columns
		for i, def := range createTable.TableDef.Cols {
			def.ColId = uint64(i)
		}
		for rkey, fkDefs := range fks {
			for constraintName, defs := range fkDefs {
				data, err := buildFkDataOfForwardRefer(ctx, constraintName, defs, createTable)
				if err != nil {
					return err
				}
				info := &plan.ForeignKeyInfo{
					Db:           rkey.Db,
					Table:        rkey.Tbl,
					ColsReferred: data.ColsReferred,
					Def:          data.Def,
				}
				createTable.FksReferToMe = append(createTable.FksReferToMe, info)
			}
		}
	}

	return nil
}

func getRefAction(typ tree.ReferenceOptionType) plan.ForeignKeyDef_RefAction {
	switch typ {
	case tree.REFERENCE_OPTION_CASCADE:
		return plan.ForeignKeyDef_CASCADE
	case tree.REFERENCE_OPTION_NO_ACTION:
		return plan.ForeignKeyDef_NO_ACTION
	case tree.REFERENCE_OPTION_RESTRICT:
		return plan.ForeignKeyDef_RESTRICT
	case tree.REFERENCE_OPTION_SET_NULL:
		return plan.ForeignKeyDef_SET_NULL
	case tree.REFERENCE_OPTION_SET_DEFAULT:
		return plan.ForeignKeyDef_SET_DEFAULT
	default:
		return plan.ForeignKeyDef_RESTRICT
	}
}

// buildFullTextIndexTable create a secondary table with schema (doc_id, word, pos) cluster by (word)
//
// with the following schema
// create __mo_secondary_xxx (
//
//	doc_id src_pk_type,
//	word varchar,
//	pos int,
//	cluster by (word)
//
// )
func buildFullTextIndexTable(createTable *plan.CreateTable, indexInfos []*tree.FullTextIndex, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string, ctx CompilerContext) error {
	if pkeyName == "" || pkeyName == catalog.FakePrimaryKeyColName {
		return moerr.NewInternalErrorNoCtx("primary key cannot be empty for fulltext index")
	}

	// check duplicate index
	if len(existedIndexes) > 0 {
		for _, existedIndex := range existedIndexes {
			if existedIndex.IndexAlgo == "fulltext" {
				for _, indexInfo := range indexInfos {
					if len(indexInfo.KeyParts) != len(existedIndex.Parts) {
						continue
					}
					n := 0
					for _, keyPart := range indexInfo.KeyParts {
						for _, ePart := range existedIndex.Parts {
							if ePart == keyPart.ColName.ColName() {
								n++
								break
							}
						}
					}

					if n == len(indexInfo.KeyParts) {
						return moerr.NewNotSupported(ctx.GetContext(), "Fulltext index are not allowed to use the same column")
					}
				}
			}
		}
	}

	for _, indexInfo := range indexInfos {
		// fulltext only support char, varchar and text
		for _, keyPart := range indexInfo.KeyParts {
			nameOrigin := keyPart.ColName.ColNameOrigin()
			name := keyPart.ColName.ColName()
			if _, ok := colMap[name]; !ok {
				return moerr.NewInvalidInput(ctx.GetContext(), fmt.Sprintf("column '%s' does not exist", nameOrigin))
			}
			typid := colMap[name].Typ.Id
			if !(typid == int32(types.T_text) || typid == int32(types.T_char) ||
				typid == int32(types.T_varchar) || typid == int32(types.T_json) || typid == int32(types.T_datalink)) {
				return moerr.NewNotSupported(ctx.GetContext(), "fulltext index only support char, varchar, text, datalink and json")
			}
		}

		// check parser
		var parsername string
		if indexInfo.IndexOption != nil && indexInfo.IndexOption.ParserName != "" {
			// set parser ngram
			parsername = strings.ToLower(indexInfo.IndexOption.ParserName)
			if parsername != "ngram" && parsername != "default" && parsername != "json" && parsername != "json_value" {
				return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("Fulltext parser %s not supported", parsername))
			}
		}
	}

	for _, indexInfo := range indexInfos {

		// create index definition
		indexDef := &plan.IndexDef{}

		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return err
		}

		indexParts := make([]string, 0)
		for _, keyPart := range indexInfo.KeyParts {
			name := keyPart.ColName.ColName()
			indexParts = append(indexParts, name)
		}

		indexDef.Unique = false
		indexDef.IndexName = indexInfo.Name
		indexDef.IndexTableName = indexTableName
		indexDef.IndexAlgo = tree.INDEX_TYPE_FULLTEXT.ToString()
		indexDef.IndexAlgoTableType = ""
		indexDef.Parts = indexParts
		indexDef.TableExist = true
		if indexInfo.IndexOption != nil {
			if indexInfo.IndexOption.ParserName != "" {
				indexDef.Option = &plan.IndexOption{ParserName: indexInfo.IndexOption.ParserName, NgramTokenSize: int32(3)}
				indexDef.IndexAlgoParams, err = catalog.IndexParamsToJsonString(indexInfo)
				if err != nil {
					return err
				}
			}
			if indexInfo.IndexOption.Comment != "" {
				indexDef.Comment = indexInfo.IndexOption.Comment
			}
		}

		// create fulltext index hidden table definition
		// doc_id, pos, word
		tableDef := &TableDef{
			Name: indexTableName,
		}

		// foreign primary key column
		keyName := catalog.FullTextIndex_TabCol_Id
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)

		// position (int32)
		keyName = catalog.FullTextIndex_TabCol_Position
		colDef = &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int32),
				Width: 32,
				Scale: -1,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)

		// word (varchar)
		keyName = catalog.FullTextIndex_TabCol_Word
		colDef = &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)

		keyName = catalog.FakePrimaryKeyColName
		colDef = &ColDef{
			Name:   keyName,
			Hidden: true,
			Alg:    plan.CompressType_Lz4,
			Typ: Type{
				Id:       int32(types.T_uint64),
				AutoIncr: true,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
			NotNull: true,
			Primary: true,
		}

		tableDef.Cols = append(tableDef.Cols, colDef)

		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}

		tableDef.ClusterBy = &ClusterByDef{
			Name: "word",
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemIndexRel,
			},
		}
		tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})

		// append to createTable.IndexTables and createTable.TableDef
		createTable.IndexTables = append(createTable.IndexTables, tableDef)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef)

	}
	return nil
}

func buildUniqueIndexTable(createTable *plan.CreateTable, indexInfos []*tree.UniqueIndex, colMap map[string]*ColDef, pkeyName string, ctx CompilerContext) error {
	for _, indexInfo := range indexInfos {
		indexDef := &plan.IndexDef{}
		indexDef.Unique = true

		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), true)

		if err != nil {
			return err
		}
		tableDef := &TableDef{
			Name: indexTableName,
		}
		indexParts := make([]string, 0)

		for _, keyPart := range indexInfo.KeyParts {
			nameOrigin := keyPart.ColName.ColNameOrigin()
			name := keyPart.ColName.ColName()
			if _, ok := colMap[name]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
			}
			if colMap[name].Typ.Id == int32(types.T_blob) {
				return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("BLOB column '%s' cannot be in index", nameOrigin))
			}
			if colMap[name].Typ.Id == int32(types.T_text) {
				return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("TEXT column '%s' cannot be in index", nameOrigin))
			}
			if colMap[name].Typ.Id == int32(types.T_datalink) {
				return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("DATALINK column '%s' cannot be in index", nameOrigin))
			}
			if colMap[name].Typ.Id == int32(types.T_json) {
				return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in index", nameOrigin))
			}
			if colMap[name].Typ.Id == int32(types.T_array_float32) || colMap[name].Typ.Id == int32(types.T_array_float64) {
				return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("VECTOR column '%s' cannot be in index", nameOrigin))
			}

			indexParts = append(indexParts, name)
		}

		var keyName string
		if len(indexInfo.KeyParts) == 1 {
			keyName = catalog.IndexTableIndexColName
			colName := indexInfo.KeyParts[0].ColName.ColName()
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ: Type{
					Id:    colMap[colName].Typ.Id,
					Width: colMap[colName].Typ.Width,
					Scale: colMap[colName].Typ.Scale,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{keyName},
				PkeyColName: keyName,
			}
		} else {
			keyName = catalog.IndexTableIndexColName
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ: Type{
					Id:    int32(types.T_varchar),
					Width: types.MaxVarcharLen,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{keyName},
				PkeyColName: keyName,
			}
		}
		if pkeyName != "" {
			colDef := &ColDef{
				Name: catalog.IndexTablePrimaryColName,
				Alg:  plan.CompressType_Lz4,
				Typ: plan.Type{
					// don't copy auto increment
					Id:    colMap[pkeyName].Typ.Id,
					Width: colMap[pkeyName].Typ.Width,
					Scale: colMap[pkeyName].Typ.Scale,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemIndexRel,
			},
		}
		tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})

		//indexDef.IndexName = indexInfo.Name
		indexDef.IndexName = indexInfo.GetIndexName()
		indexDef.IndexTableName = indexTableName
		indexDef.Parts = indexParts
		indexDef.TableExist = true
		if indexInfo.IndexOption != nil {
			indexDef.Comment = indexInfo.IndexOption.Comment
		} else {
			indexDef.Comment = ""
		}
		createTable.IndexTables = append(createTable.IndexTables, tableDef)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef)
	}
	return nil
}

func buildSecondaryIndexDef(createTable *plan.CreateTable, indexInfos []*tree.Index, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string, ctx CompilerContext) (err error) {
	if len(pkeyName) == 0 {
		return moerr.NewInternalErrorNoCtx("primary key cannot be empty for secondary index")
	}

	for _, indexInfo := range indexInfos {
		err = checkIndexKeypartSupportability(ctx.GetContext(), indexInfo.KeyParts)
		if err != nil {
			return err
		}

		var indexDef []*plan.IndexDef
		var tableDef []*TableDef
		switch indexInfo.KeyType {
		case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID:
			indexDef, tableDef, err = buildRegularSecondaryIndexDef(ctx, indexInfo, colMap, pkeyName)
		case tree.INDEX_TYPE_IVFFLAT:
			indexDef, tableDef, err = buildIvfFlatSecondaryIndexDef(ctx, indexInfo, colMap, existedIndexes, pkeyName)
		case tree.INDEX_TYPE_MASTER:
			indexDef, tableDef, err = buildMasterSecondaryIndexDef(ctx, indexInfo, colMap, pkeyName)
		case tree.INDEX_TYPE_HNSW:
			indexDef, tableDef, err = buildHnswSecondaryIndexDef(ctx, indexInfo, colMap, existedIndexes, pkeyName)
		default:
			return moerr.NewInvalidInputNoCtxf("unsupported index type: %s", indexInfo.KeyType.ToString())
		}

		if err != nil {
			return err
		}
		createTable.IndexTables = append(createTable.IndexTables, tableDef...)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef...)

	}
	return nil
}

// buildMasterSecondaryIndexDef will create hidden internal table with schema.
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col varchar,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
func buildMasterSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {
	// 1. indexDef init
	indexDef := &plan.IndexDef{}
	indexDef.Unique = false

	// 2. tableDef init
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	tableDef := &TableDef{
		Name: indexTableName,
	}

	nameCount := make(map[string]int)
	// Note: Index Parts will store the ColName, as Parts is used to populate mo_index_table.
	// However, when inserting Index, we convert Parts (ie ColName) to ColIdx.
	indexParts := make([]string, 0)

	for _, keyPart := range indexInfo.KeyParts {
		nameOrigin := keyPart.ColName.ColNameOrigin()
		name := keyPart.ColName.ColName()
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
		}
		if colMap[name].Typ.Id != int32(types.T_varchar) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("column '%s' is not varchar type.", nameOrigin))
		}
		indexParts = append(indexParts, name)
	}

	var keyName = catalog.MasterIndexTableIndexColName
	colDef := &ColDef{
		Name: keyName,
		Alg:  plan.CompressType_Lz4,
		Typ: Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
	tableDef.Cols = append(tableDef.Cols, colDef)
	tableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{keyName},
		PkeyColName: keyName,
	}
	if pkeyName != "" {
		pkColDef := &ColDef{
			Name: catalog.MasterIndexTablePrimaryColName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, pkColDef)
	}

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemIndexRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})

	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.TableExist = true
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = ""

	if indexInfo.IndexOption != nil {
		indexDef.Comment = indexInfo.IndexOption.Comment

		params, err := catalog.IndexParamsToJsonString(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
	}
	return []*plan.IndexDef{indexDef}, []*TableDef{tableDef}, nil
}

// buildRegularSecondingIndexDef will create a hidden index table with schema
//
// when number of primary key == 1
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col src_pk_type,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
//
// when number of primary key > 1
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col varchar,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
func buildRegularSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {

	// 1. indexDef init
	indexDef := &plan.IndexDef{}
	indexDef.Unique = false

	// 2. tableDef init
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	tableDef := &TableDef{
		Name: indexTableName,
	}

	nameCount := make(map[string]int)
	indexParts := make([]string, 0)

	isPkAlreadyPresentInIndexParts := false
	for _, keyPart := range indexInfo.KeyParts {
		name := keyPart.ColName.ColName()
		nameOrigin := keyPart.ColName.ColNameOrigin()
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
		}
		if colMap[name].Typ.Id == int32(types.T_blob) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("BLOB column '%s' cannot be in index", nameOrigin))
		}
		if colMap[name].Typ.Id == int32(types.T_text) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("TEXT column '%s' cannot be in index", nameOrigin))
		}
		if colMap[name].Typ.Id == int32(types.T_datalink) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("DATALINK column '%s' cannot be in index", nameOrigin))
		}
		if colMap[name].Typ.Id == int32(types.T_json) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in index", nameOrigin))
		}
		if colMap[name].Typ.Id == int32(types.T_array_float32) || colMap[name].Typ.Id == int32(types.T_array_float64) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("VECTOR column '%s' cannot be in index", nameOrigin))
		}

		if strings.Compare(name, pkeyName) == 0 || catalog.IsAlias(name) {
			isPkAlreadyPresentInIndexParts = true
		}
		indexParts = append(indexParts, name)
	}

	if !isPkAlreadyPresentInIndexParts {
		indexParts = append(indexParts, catalog.CreateAlias(pkeyName))
	}

	var keyName string
	if len(indexParts) == 1 {
		// This means indexParts only contains the primary key column
		keyName = catalog.IndexTableIndexColName
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}
	} else {
		keyName = catalog.IndexTableIndexColName
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}
	}
	if pkeyName != "" {
		colDef := &ColDef{
			Name: catalog.IndexTablePrimaryColName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
	}

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemIndexRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})

	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.TableExist = true
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = ""

	if indexInfo.IndexOption != nil {
		indexDef.Comment = indexInfo.IndexOption.Comment

		params, err := catalog.IndexParamsToJsonString(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
	}
	return []*plan.IndexDef{indexDef}, []*TableDef{tableDef}, nil
}

// buildIvfFlatSecondIndexDef create three internal tables
//
// with the following schemas,
//
// create __mo_secondary_metadata (
//	__mo_index_key varchar,
//	__mo_index_val varhcar,
// 	primary key __mo_index_key,
//)
//
// create __mo_secondary_centroids (
//	__mo_index_centroid_version bigint,
//	__mo_index_centroid_id bigint,
//	__mo_index_centroid vecf32 or vecf64,
//	primary key (__mo_index_centroid_version, __mo_index_centroid_id),
// )
// create __mo_seconary_entries (
//	__mo_index_centroid_fk_version bigint,
//	__mo_index_centroid_fk_id bigint,
//	__mo_index_pri_col src_pk_type,
//	__mo_index_centroid_fk_entry vecf32 or vecf64,
//	primary key (__mo_index_centriod_fk_version, __mo_index_centroid_fk_id, __mo_index_pri_col)
// )

func buildIvfFlatSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {

	indexParts := make([]string, 1)

	// 0. Validate: We only support 1 column of either VECF32 or VECF64 type
	{
		if len(indexInfo.KeyParts) != 1 {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "don't support multi column  IVF vector index")
		}

		name := indexInfo.KeyParts[0].ColName.ColName()
		indexParts[0] = name

		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
		}
		if colMap[name].Typ.Id != int32(types.T_array_float32) && colMap[name].Typ.Id != int32(types.T_array_float64) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "IVFFLAT only supports VECFXX column types")
		}

		if len(existedIndexes) > 0 {
			for _, existedIndex := range existedIndexes {
				if existedIndex.IndexAlgo == "ivfflat" && existedIndex.Parts[0] == name {
					return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple IVFFLAT indexes are not allowed to use the same column")
				}
			}
		}

	}

	indexDefs := make([]*plan.IndexDef, 3)
	tableDefs := make([]*TableDef, 3)

	// 1. create ivf-flat `metadata` table
	{
		// 1.a tableDef1 init
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[0] = &TableDef{
			Name:      indexTableName,
			TableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
			Cols:      make([]*ColDef, 2),
		}

		// 1.b indexDef1 init
		indexDefs[0], err = CreateIndexDef(indexInfo, indexTableName, catalog.SystemSI_IVFFLAT_TblType_Metadata, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		// 1.c columns: key (PK), val
		tableDefs[0].Cols[0] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Primary: true,
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[0].Cols[1] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}

		// 1.d PK def
		tableDefs[0].Pkey = &PrimaryKeyDef{
			Names:       []string{catalog.SystemSI_IVFFLAT_TblCol_Metadata_key},
			PkeyColName: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemSI_IVFFLAT_TblType_Metadata,
			},
		}
		tableDefs[0].Defs = append(tableDefs[0].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	// 2. create ivf-flat `centroids` table
	colName := indexInfo.KeyParts[0].ColName.ColName()
	{
		// 2.a tableDefs[1] init
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[1] = &TableDef{
			Name:      indexTableName,
			TableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
			Cols:      make([]*ColDef, 4),
		}

		// 2.b indexDefs[1] init
		indexDefs[1], err = CreateIndexDef(indexInfo, indexTableName, catalog.SystemSI_IVFFLAT_TblType_Centroids, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		// 2.c columns: version, id, centroid, PRIMARY KEY (version,id)
		tableDefs[1].Cols[0] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[1].Cols[1] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[1].Cols[2] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    colMap[colName].Typ.Id,
				Width: colMap[colName].Typ.Width,
				Scale: colMap[colName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[1].Cols[3] = MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
		tableDefs[1].Cols[3].Alg = plan.CompressType_Lz4
		tableDefs[1].Cols[3].Primary = true

		// 2.d PK def
		tableDefs[1].Pkey = &PrimaryKeyDef{
			Names: []string{
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[1].Cols[3],
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemSI_IVFFLAT_TblType_Centroids,
			},
		}
		tableDefs[1].Defs = append(tableDefs[1].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	// 3. create ivf-flat `entries` table
	{
		// 3.a tableDefs[2] init
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[2] = &TableDef{
			Name:      indexTableName,
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
			Cols:      make([]*ColDef, 5),
		}

		// 3.b indexDefs[2] init
		indexDefs[2], err = CreateIndexDef(indexInfo, indexTableName, catalog.SystemSI_IVFFLAT_TblType_Entries, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		// 3.c columns: version, id, origin_pk, PRIMARY KEY (version,origin_pk)
		tableDefs[2].Cols[0] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[2].Cols[1] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}

		tableDefs[2].Cols[2] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				//NOTE: don't directly copy the Type from Original Table's PK column.
				// If you do that, we can get the AutoIncrement property from the original table's PK column.
				// This results in a bug when you try to insert data into entries table.
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[2].Cols[3] = &ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    colMap[colName].Typ.Id,
				Width: colMap[colName].Typ.Width,
				Scale: colMap[colName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}

		tableDefs[2].Cols[4] = MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
		tableDefs[2].Cols[4].Alg = plan.CompressType_Lz4
		tableDefs[2].Cols[4].Primary = true

		// 3.d PK def
		tableDefs[2].Pkey = &PrimaryKeyDef{
			Names: []string{
				catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, // added to make this unique
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[2].Cols[4],
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemSI_IVFFLAT_TblType_Entries,
			},
		}
		tableDefs[2].Defs = append(tableDefs[2].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	return indexDefs, tableDefs, nil
}

// buildHnswSecondaryIndexDef will create two internal tables
//
// with the following schemas:
//
// create __mo_secondary_metadata (
//
//	index_id varchar,
//	checksum varchar,
//	timestamp int64,
//	filesize int64,
//	primary key index_id
//
// )
//
// create __mo_secondary_index (
//
//	index_id varchar,
//	chunk_id int64,
// 	data blob,
//	tag int64,
//	primary key (index_id, chunk_id)
// )

func buildHnswSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {

	if pkeyName == "" || pkeyName == catalog.FakePrimaryKeyColName {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key cannot be empty for fulltext index")
	}

	if colMap[pkeyName].Typ.Id != int32(types.T_int64) {
		return nil, nil, moerr.NewInternalErrorNoCtx("type of primary key must be bigint")
	}

	indexParts := make([]string, 1)

	// 0. Validate: We only support 1 column of VECF32
	{
		if len(indexInfo.KeyParts) != 1 {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "don't support multi column  HNSW vector index")
		}

		name := indexInfo.KeyParts[0].ColName.ColName()
		indexParts[0] = name

		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
		}
		if colMap[name].Typ.Id != int32(types.T_array_float32) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "HNSW only supports VECF32 column types")
		}

		if len(existedIndexes) > 0 {
			for _, existedIndex := range existedIndexes {
				if existedIndex.IndexAlgo == "hnsw" && existedIndex.Parts[0] == name {
					return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple HNSW indexes are not allowed to use the same column")
				}
			}
		}

	}

	indexDefs := make([]*plan.IndexDef, 2)
	tableDefs := make([]*TableDef, 2)

	// 1. create hnsw `metadata` table
	{
		// 1.a tableDef1 init
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[0] = &TableDef{
			Name:      indexTableName,
			TableType: catalog.Hnsw_TblType_Metadata,
			Cols:      make([]*ColDef, 4),
		}

		// 1.b indexDef1 init
		indexDefs[0], err = CreateIndexDef(indexInfo, indexTableName, catalog.Hnsw_TblType_Metadata, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		// 1.c columns: key (PK), val
		tableDefs[0].Cols[0] = &ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Index_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_varchar),
				Width: 128,
				Scale: 0,
			},
			Primary: true,
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[0].Cols[1] = &ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Checksum,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[0].Cols[2] = &ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Timestamp,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[0].Cols[3] = &ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Filesize,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}

		// 1.d PK def
		tableDefs[0].Pkey = &PrimaryKeyDef{
			Names:       []string{catalog.Hnsw_TblCol_Metadata_Index_Id},
			PkeyColName: catalog.Hnsw_TblCol_Metadata_Index_Id,
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.Hnsw_TblType_Metadata,
			},
		}
		tableDefs[0].Defs = append(tableDefs[0].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	// 2. create hnsw storage table
	//colName := indexInfo.KeyParts[0].ColName.ColName()
	{
		// 1.a tableDef1 init
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[1] = &TableDef{
			Name:      indexTableName,
			TableType: catalog.Hnsw_TblType_Storage,
			Cols:      make([]*ColDef, 5),
		}

		// 1.b indexDef1 init
		indexDefs[1], err = CreateIndexDef(indexInfo, indexTableName, catalog.Hnsw_TblType_Storage, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		// 1.c columns: key (PK), val
		tableDefs[1].Cols[0] = &ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Index_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_varchar),
				Width: 128,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[1].Cols[1] = &ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Chunk_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[1].Cols[2] = &ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Data,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_blob),
				Width: 65536,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDefs[1].Cols[3] = &ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Tag,
			Alg:  plan.CompressType_Lz4,
			Typ: Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}

		tableDefs[1].Cols[4] = MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
		tableDefs[1].Cols[4].Alg = plan.CompressType_Lz4
		tableDefs[1].Cols[4].Primary = true

		tableDefs[1].Pkey = &PrimaryKeyDef{
			Names: []string{catalog.Hnsw_TblCol_Storage_Index_Id,
				catalog.Hnsw_TblCol_Storage_Chunk_Id},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[1].Cols[3],
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.Hnsw_TblType_Storage,
			},
		}
		tableDefs[1].Defs = append(tableDefs[1].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}
	return indexDefs, tableDefs, nil
}

func CreateIndexDef(indexInfo *tree.Index,
	indexTableName, indexAlgoTableType string,
	indexParts []string, isUnique bool) (*plan.IndexDef, error) {

	//TODO: later use this function for RegularSecondaryIndex and UniqueIndex.

	indexDef := &plan.IndexDef{}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts

	indexDef.Unique = isUnique
	indexDef.TableExist = true

	// Algorithm related fields
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = indexAlgoTableType
	if indexInfo.IndexOption != nil {
		// Copy Comment as it is
		indexDef.Comment = indexInfo.IndexOption.Comment

		// Create params JSON string and set it
		params, err := catalog.IndexParamsToJsonString(indexInfo)
		if err != nil {
			return nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		// default indexInfo.IndexOption values
		switch indexInfo.KeyType {
		case catalog.MoIndexDefaultAlgo, catalog.MoIndexBTreeAlgo:
			indexDef.Comment = ""
			indexDef.IndexAlgoParams = ""
		case catalog.MOIndexMasterAlgo:
			indexDef.Comment = ""
			indexDef.IndexAlgoParams = ""
		case catalog.MoIndexIvfFlatAlgo:
			var err error
			indexDef.IndexAlgoParams, err = catalog.IndexParamsMapToJsonString(catalog.DefaultIvfIndexAlgoOptions())
			if err != nil {
				return nil, err
			}
		case catalog.MoIndexHnswAlgo:
			indexDef.Comment = ""
			indexDef.IndexAlgoParams = ""
		}

	}

	nameCount := make(map[string]int)
	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	return indexDef, nil
}

func buildTruncateTable(stmt *tree.TruncateTable, ctx CompilerContext) (*Plan, error) {
	truncateTable := &plan.TruncateTable{}

	truncateTable.Database = string(stmt.Name.SchemaName)
	if truncateTable.Database == "" {
		truncateTable.Database = ctx.DefaultDatabase()
	}
	truncateTable.Table = string(stmt.Name.ObjectName)
	obj, tableDef, err := ctx.Resolve(truncateTable.Database, truncateTable.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
	} else {
		if tableDef.TableType == catalog.SystemSourceRel {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate source '%v' ", truncateTable.Table)
		}

		if len(tableDef.RefChildTbls) > 0 {
			//if all children tables are self reference, we can drop the table
			if !HasFkSelfReferOnly(tableDef) {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate table '%v' referenced by some foreign key constraint", truncateTable.Table)
			}
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
		}

		truncateTable.TableId = tableDef.TblId
		if tableDef.Fkeys != nil {
			for _, fk := range tableDef.Fkeys {
				truncateTable.ForeignTbl = append(truncateTable.ForeignTbl, fk.ForeignTbl)
			}
		}

		truncateTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		//non-sys account can not truncate the cluster table
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if truncateTable.GetClusterTable().GetIsClusterTable() && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can truncate the cluster table")
		}

		if obj.PubInfo != nil {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate table '%v' which is published by other account", truncateTable.Table)
		}

		truncateTable.IndexTableNames = make([]string, 0)
		if tableDef.Indexes != nil {
			for _, indexdef := range tableDef.Indexes {
				// We only handle truncate on regular index. For other indexes such as IVF, we don't handle truncate now.
				if indexdef.TableExist && catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
				} else if indexdef.TableExist && catalog.IsIvfIndexAlgo(indexdef.IndexAlgo) {
					if indexdef.IndexAlgoTableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
						//TODO: check with @feng on how to handle truncate on IVF index
						// Right now, we are only clearing the entries. Should we empty the centroids and metadata as well?
						// Ideally, after truncate the user is expected to run re-index.
						truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
					}
				} else if indexdef.TableExist && catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
				} else if indexdef.TableExist && catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo) {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
				} else if indexdef.TableExist && catalog.IsHnswIndexAlgo(indexdef.IndexAlgo) {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
				}
			}
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_TRUNCATE_TABLE,
				Definition: &plan.DataDefinition_TruncateTable{
					TruncateTable: truncateTable,
				},
			},
		},
	}, nil
}

func buildDropTable(stmt *tree.DropTable, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) tables in one statement", len(stmt.Names))
	}

	dropTable.Database = string(stmt.Names[0].SchemaName)

	// If the database name is empty, attempt to get default database name
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}

	// If the final database name is still empty, return an error
	if dropTable.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	dropTable.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropTable.Database, dropTable.Table, nil)
	if err != nil {
		return nil, err
	}

	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	} else {
		if obj.PubInfo != nil {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop subscription table %s", dropTable.Table)
		}

		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if enabled && len(tableDef.RefChildTbls) > 0 {
			//if all children tables are self reference, we can drop the table
			if !HasFkSelfReferOnly(tableDef) {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop table '%v' referenced by some foreign key constraint", dropTable.Table)
			}
		}

		isView := (tableDef.ViewSql != nil)
		dropTable.IsView = isView

		if isView && !dropTable.IfExists {
			// drop table v0, v0 is view
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		} else if isView {
			// drop table if exists v0, v0 is view
			dropTable.Table = ""
		}

		// Can not use drop table to drop sequence.
		if tableDef.TableType == catalog.SystemSequenceRel && !dropTable.IfExists {
			return nil, moerr.NewInternalError(ctx.GetContext(), "Should use 'drop sequence' to drop a sequence")
		} else if tableDef.TableType == catalog.SystemSequenceRel {
			// If exists, don't drop anything.
			dropTable.Table = ""
		}

		dropTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		//non-sys account can not drop the cluster table
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if dropTable.GetClusterTable().GetIsClusterTable() && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can drop the cluster table")
		}

		ignore := false
		val := ctx.GetContext().Value(defines.IgnoreForeignKey{})
		if val != nil {
			ignore = val.(bool)
		}

		dropTable.TableId = tableDef.TblId
		if tableDef.Fkeys != nil && !ignore {
			for _, fk := range tableDef.Fkeys {
				if fk.ForeignTbl == 0 {
					continue
				}
				dropTable.ForeignTbl = append(dropTable.ForeignTbl, fk.ForeignTbl)
			}
		}

		// collect child tables that needs remove fk relationships
		// with the table
		if tableDef.RefChildTbls != nil && !ignore {
			for _, childTbl := range tableDef.RefChildTbls {
				if childTbl == 0 {
					continue
				}
				dropTable.FkChildTblsReferToMe = append(dropTable.FkChildTblsReferToMe, childTbl)
			}
		}

		dropTable.IndexTableNames = make([]string, 0)
		if tableDef.Indexes != nil {
			for _, indexdef := range tableDef.Indexes {
				if indexdef.TableExist {
					dropTable.IndexTableNames = append(dropTable.IndexTableNames, indexdef.IndexTableName)
				}
			}
		}

		dropTable.TableDef = tableDef
		dropTable.UpdateFkSqls = []string{getSqlForDeleteTable(dropTable.Database, dropTable.Table)}
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildDropView(stmt *tree.DropView, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) view", len(stmt.Names))
	}

	dropTable.Database = string(stmt.Names[0].SchemaName)

	// If the database name is empty, attempt to get default database name
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	// If the final database name is still empty, return an error
	if dropTable.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	dropTable.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropTable.Database, dropTable.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	} else {
		if tableDef.ViewSql == nil {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop view in subscription database")
		}
	}
	dropTable.IsView = true

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildCreateDatabase(stmt *tree.CreateDatabase, ctx CompilerContext) (*Plan, error) {
	if string(stmt.Name) == defines.TEMPORARY_DBNAME {
		return nil, moerr.NewInternalError(ctx.GetContext(), "this database name is used by mo temporary engine")
	}
	createDB := &plan.CreateDatabase{
		IfNotExists: stmt.IfNotExists,
		Database:    string(stmt.Name),
	}

	if stmt.SubscriptionOption != nil {
		accName := string(stmt.SubscriptionOption.From)
		pubName := string(stmt.SubscriptionOption.Publication)
		subName := string(stmt.Name)
		if err := ctx.CheckSubscriptionValid(subName, accName, pubName); err != nil {
			return nil, err
		}
		createDB.SubscriptionOption = &plan.SubscriptionOption{
			From:        string(stmt.SubscriptionOption.From),
			Publication: string(stmt.SubscriptionOption.Publication),
		}
	}
	createDB.Sql = stmt.Sql

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_DATABASE,
				Definition: &plan.DataDefinition_CreateDatabase{
					CreateDatabase: createDB,
				},
			},
		},
	}, nil
}

func buildDropDatabase(stmt *tree.DropDatabase, ctx CompilerContext) (*Plan, error) {
	dropDB := &plan.DropDatabase{
		IfExists: stmt.IfExists,
		Database: string(stmt.Name),
	}
	if publishing, err := ctx.IsPublishing(dropDB.Database); err != nil {
		return nil, err
	} else if publishing {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop database '%v' which is publishing", dropDB.Database)
	}

	if ctx.DatabaseExists(string(stmt.Name), nil) {
		databaseId, err := ctx.GetDatabaseId(string(stmt.Name), nil)
		if err != nil {
			return nil, err
		}
		dropDB.DatabaseId = databaseId

		//check foreign keys exists or not
		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if enabled {
			dropDB.CheckFKSql = getSqlForCheckHasDBRefersTo(dropDB.Database)
		}
	}

	dropDB.UpdateFkSql = getSqlForDeleteDB(dropDB.Database)

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_DATABASE,
				Definition: &plan.DataDefinition_DropDatabase{
					DropDatabase: dropDB,
				},
			},
		},
	}, nil
}

// In MySQL, the CREATE INDEX syntax can only create one index instance at a time
func buildCreateIndex(stmt *tree.CreateIndex, ctx CompilerContext) (*Plan, error) {
	createIndex := &plan.CreateIndex{}
	if len(stmt.Table.SchemaName) == 0 {
		createIndex.Database = ctx.DefaultDatabase()
	} else {
		createIndex.Database = string(stmt.Table.SchemaName)
	}
	// check table
	tableName := string(stmt.Table.ObjectName)
	obj, tableDef, err := ctx.Resolve(createIndex.Database, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), createIndex.Database, tableName)
	}
	if obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create index in subscription database")
	}
	// check index
	indexName := string(stmt.Name)
	for _, def := range tableDef.Indexes {
		if def.IndexName == indexName {
			return nil, moerr.NewDuplicateKey(ctx.GetContext(), indexName)
		}
	}
	// build index
	var ftIdx *tree.FullTextIndex
	var uIdx *tree.UniqueIndex
	var sIdx *tree.Index
	switch stmt.IndexCat {
	case tree.INDEX_CATEGORY_UNIQUE:
		uIdx = &tree.UniqueIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	case tree.INDEX_CATEGORY_NONE:
		sIdx = &tree.Index{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
			KeyType:     stmt.IndexOption.IType,
		}
	case tree.INDEX_CATEGORY_FULLTEXT:
		ftIdx = &tree.FullTextIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	default:
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}
	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}

	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		colMap[tableDef.Pkey.CompPkeyCol.Name] = tableDef.Pkey.CompPkeyCol
	}

	// index.TableDef.Defs store info of index need to be modified
	// index.IndexTables store index table need to be created
	oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
	createIndex.OriginTablePrimaryKey = oriPriKeyName

	indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
	if uIdx != nil {
		if err := buildUniqueIndexTable(indexInfo, []*tree.UniqueIndex{uIdx}, colMap, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if ftIdx != nil {
		if err := buildFullTextIndexTable(indexInfo, []*tree.FullTextIndex{ftIdx}, colMap, tableDef.Indexes, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if sIdx != nil {
		if err := buildSecondaryIndexDef(indexInfo, []*tree.Index{sIdx}, colMap, tableDef.Indexes, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	createIndex.Index = indexInfo
	createIndex.Table = tableName
	createIndex.TableDef = tableDef

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_INDEX,
				Definition: &plan.DataDefinition_CreateIndex{
					CreateIndex: createIndex,
				},
			},
		},
	}, nil
}

func buildDropIndex(stmt *tree.DropIndex, ctx CompilerContext) (*Plan, error) {
	dropIndex := &plan.DropIndex{}
	if len(stmt.TableName.SchemaName) == 0 {
		dropIndex.Database = ctx.DefaultDatabase()
	} else {
		dropIndex.Database = string(stmt.TableName.SchemaName)
	}

	// If the final database name is still empty, return an error
	if dropIndex.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	// check table
	dropIndex.Table = string(stmt.TableName.ObjectName)
	obj, tableDef, err := ctx.Resolve(dropIndex.Database, dropIndex.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropIndex.Database, dropIndex.Table)
	}

	if obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop index in subscription database")
	}

	// check index
	dropIndex.IndexName = string(stmt.Name)
	found := false

	for _, indexdef := range tableDef.Indexes {
		if dropIndex.IndexName == indexdef.IndexName {
			dropIndex.IndexTableName = indexdef.IndexTableName
			found = true
			break
		}
	}

	if !found {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "not found index: %s", dropIndex.IndexName)
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_INDEX,
				Definition: &plan.DataDefinition_DropIndex{
					DropIndex: dropIndex,
				},
			},
		},
	}, nil
}

// Get tabledef(col, viewsql, properties) for alterview.
func buildAlterView(stmt *tree.AlterView, ctx CompilerContext) (*Plan, error) {
	viewName := string(stmt.Name.ObjectName)
	alterView := &plan.AlterView{
		IfExists: stmt.IfExists,
		TableDef: &plan.TableDef{
			Name: viewName,
		},
	}
	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		alterView.Database = ""
	} else {
		alterView.Database = string(stmt.Name.SchemaName)
	}
	if alterView.Database == "" {
		alterView.Database = ctx.DefaultDatabase()
	}

	//step 1: check the view exists or not
	obj, oldViewDef, err := ctx.Resolve(alterView.Database, viewName, nil)
	if err != nil {
		return nil, err
	}
	if oldViewDef == nil {
		if !alterView.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(),
				alterView.Database,
				viewName)
		}
	} else {
		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter view in subscription database")
		}
		if oldViewDef.ViewSql == nil {
			return nil, moerr.NewBadView(ctx.GetContext(),
				alterView.Database,
				viewName)
		}
	}

	//step 2: generate new view def
	ctx.SetBuildingAlterView(true, alterView.Database, viewName)
	//restore
	defer func() {
		ctx.SetBuildingAlterView(false, "", "")
	}()
	tableDef, err := genViewTableDef(ctx, stmt.AsSource)
	if err != nil {
		return nil, err
	}

	alterView.TableDef.Cols = tableDef.Cols
	alterView.TableDef.ViewSql = tableDef.ViewSql
	alterView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_VIEW,
				Definition: &plan.DataDefinition_AlterView{
					AlterView: alterView,
				},
			},
		},
	}, nil
}

func buildRenameTable(stmt *tree.RenameTable, ctx CompilerContext) (*Plan, error) {

	alterTables := stmt.AlterTables
	renameTables := make([]*plan.AlterTable, 0)
	for _, alterTable := range alterTables {
		schemaName, tableName := string(alterTable.Table.Schema()), string(alterTable.Table.Name())
		if schemaName == "" {
			schemaName = ctx.DefaultDatabase()
		}
		objRef, tableDef, err := ctx.Resolve(schemaName, tableName, nil)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
		}

		if tableDef.IsTemporary {
			return nil, moerr.NewNYI(ctx.GetContext(), "alter table for temporary table")
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
		}
		if objRef.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
		}
		isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if isClusterTable && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
		}

		alterTablePlan := &plan.AlterTable{
			Actions:        make([]*plan.AlterTable_Action, len(alterTable.Options)),
			AlgorithmType:  plan.AlterTable_INPLACE,
			Database:       schemaName,
			TableDef:       tableDef,
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		var updateSqls []string
		for i, option := range alterTable.Options {
			switch opt := option.(type) {
			case *tree.AlterOptionTableName:
				oldName := tableDef.Name
				newName := string(opt.Name.ToTableName().ObjectName)
				if oldName != newName {
					_, tableDef, err := ctx.Resolve(schemaName, newName, nil)
					if err != nil {
						return nil, err
					}
					if tableDef != nil {
						return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
					}
				}
				alterTablePlan.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterName{
						AlterName: &plan.AlterTableName{
							OldName: oldName,
							NewName: newName,
						},
					},
				}
				updateSqls = append(updateSqls, getSqlForRenameTable(schemaName, oldName, newName)...)

			default:
				// return err
				return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
			}
			alterTablePlan.UpdateFkSqls = updateSqls
		}
		renameTables = append(renameTables, alterTablePlan)
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_RENAME_TABLE,
				Definition: &plan.DataDefinition_RenameTable{
					RenameTable: &plan.RenameTable{
						AlterTables: renameTables,
					},
				},
			},
		},
	}, nil
}

func formatTreeNode(opt tree.NodeFormatter) string {
	// get callsite
	ft := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
	opt.Format(ft)
	return ft.String()
}

func buildAlterTableInplace(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	tableName := string(stmt.Table.ObjectName)
	databaseName := string(stmt.Table.SchemaName)
	if databaseName == "" {
		databaseName = ctx.DefaultDatabase()
	}

	_, tableDef, err := ctx.Resolve(databaseName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), databaseName, tableName)
	}

	alterTable := &plan.AlterTable{
		Actions:        make([]*plan.AlterTable_Action, len(stmt.Options)),
		AlgorithmType:  plan.AlterTable_INPLACE,
		Database:       databaseName,
		TableDef:       tableDef,
		IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		RawSQL: tree.StringWithOpts(
			stmt,
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithSingleQuoteString(),
		),
	}
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if alterTable.IsClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(
			ctx.GetContext(),
			"only the sys account can alter the cluster table",
		)
	}

	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}
	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		colMap[tableDef.Pkey.CompPkeyCol.Name] = tableDef.Pkey.CompPkeyCol
	}
	unsupportedErrFmt := "unsupported alter option in inplace mode: %s"

	var detectSqls []string
	var updateSqls []string
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	for i, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.AlterOptionDrop:
			alterTableDrop := new(plan.AlterTableDrop)
			// lower case
			constraintName := string(opt.Name)
			if constraintNameAreWhiteSpaces(constraintName) {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't DROP '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTableDrop.Name = constraintName
			name_not_found := true
			switch opt.Typ {
			case tree.AlterTableDropIndex, tree.AlterTableDropKey:
				alterTableDrop.Typ = plan.AlterTableDrop_INDEX
				// check index
				for _, indexdef := range tableDef.Indexes {
					if constraintName == indexdef.IndexName {
						name_not_found = false
						break
					}
				}
			case tree.AlterTableDropForeignKey:
				alterTableDrop.Typ = plan.AlterTableDrop_FOREIGN_KEY
				for _, fk := range tableDef.Fkeys {
					if fk.Name == constraintName {
						name_not_found = false
						updateSqls = append(
							updateSqls,
							getSqlForDeleteConstraint(
								databaseName,
								tableName,
								constraintName,
							),
						)
						break
					}
				}
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					formatTreeNode(opt),
				)
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't DROP '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_Drop{
					Drop: alterTableDrop,
				},
			}

		case *tree.AlterOptionAdd:
			switch def := opt.Def.(type) {
			case *tree.ForeignKey:
				err = adjustConstraintName(ctx.GetContext(), def)
				if err != nil {
					return nil, err
				}

				fkData, err := getForeignKeyData(ctx, databaseName, tableDef, def)
				if err != nil {
					return nil, err
				}
				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddFk{
						AddFk: &plan.AlterTableAddFk{
							DbName:    fkData.ParentDbName,
							TableName: fkData.ParentTableName,
							Cols:      fkData.Cols.Cols,
							Fkey:      fkData.Def,
						},
					},
				}
				//for new fk in this alter table, the data in the table must
				//be checked to confirm that it is compliant with foreign key constraints.
				if fkData.IsSelfRefer {
					//fk self refer.
					//check columns of fk self refer are valid
					err = checkFkColsAreValid(ctx, fkData, tableDef)
					if err != nil {
						return nil, err
					}
					sqls, err := genSqlsForCheckFKSelfRefer(
						ctx.GetContext(),
						databaseName,
						tableDef.Name,
						tableDef.Cols,
						[]*plan.ForeignKeyDef{fkData.Def},
					)
					if err != nil {
						return nil, err
					}
					detectSqls = append(detectSqls, sqls...)
				} else {
					//get table def of parent table
					_, parentTableDef, err := ctx.Resolve(
						fkData.ParentDbName,
						fkData.ParentTableName,
						nil,
					)
					if err != nil {
						return nil, err
					}
					if parentTableDef == nil {
						return nil, moerr.NewNoSuchTable(
							ctx.GetContext(),
							fkData.ParentDbName,
							fkData.ParentTableName,
						)
					}
					sql, err := genSqlForCheckFKConstraints(
						ctx.GetContext(),
						fkData.Def,
						databaseName, tableDef.Name, tableDef.Cols,
						fkData.ParentDbName,
						fkData.ParentTableName,
						parentTableDef.Cols,
					)
					if err != nil {
						return nil, err
					}
					detectSqls = append(detectSqls, sql)
				}
				updateSqls = append(updateSqls, fkData.UpdateSql)
			case *tree.UniqueIndex:
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.GetIndexName()
				constrNames := map[string]bool{}
				// Check not empty constraint name whether is duplicated.
				for _, idx := range tableDef.Indexes {
					nameLower := strings.ToLower(idx.IndexName)
					constrNames[nameLower] = true
				}

				if err := checkDuplicateConstraint(
					constrNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}
				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyUniqueIndexName(constrNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildUniqueIndexTable(
					indexInfo,
					[]*tree.UniqueIndex{def},
					colMap,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			case *tree.FullTextIndex:
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.Name
				constrNames := map[string]bool{}
				// Check not empty constraint name whether is duplicated.
				for _, idx := range tableDef.Indexes {
					nameLower := strings.ToLower(idx.IndexName)
					constrNames[nameLower] = true
				}

				if err := checkDuplicateConstraint(
					constrNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}

				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyFullTextIndexName(constrNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildFullTextIndexTable(
					indexInfo,
					[]*tree.FullTextIndex{def},
					colMap,
					tableDef.Indexes,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			case *tree.Index:
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.Name

				constrNames := map[string]bool{}
				// Check not empty constraint name whether is duplicated.
				for _, idx := range tableDef.Indexes {
					nameLower := strings.ToLower(idx.IndexName)
					constrNames[nameLower] = true
				}

				if err := checkDuplicateConstraint(
					constrNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}

				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyIndexName(constrNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)

				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildSecondaryIndexDef(
					indexInfo,
					[]*tree.Index{def},
					colMap,
					tableDef.Indexes,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					formatTreeNode(def),
				)
			}

		case *tree.AlterOptionAlterIndex:
			alterTableIndex := new(plan.AlterTableAlterIndex)
			constraintName := string(opt.Name)
			alterTableIndex.IndexName = constraintName
			alterTableIndex.Visible = opt.Visibility == tree.VISIBLE_TYPE_VISIBLE

			name_not_found := true
			// check index
			for _, indexdef := range tableDef.Indexes {
				if constraintName == indexdef.IndexName {
					name_not_found = false
					break
				}
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't ALTER '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterIndex{
					AlterIndex: alterTableIndex,
				},
			}

		case *tree.AlterOptionAlterReIndex:
			alterTableReIndex := new(plan.AlterTableAlterReIndex)
			constraintName := string(opt.Name)
			alterTableReIndex.IndexName = constraintName

			switch opt.KeyType {
			case tree.INDEX_TYPE_IVFFLAT:
				if opt.AlgoParamList <= 0 {
					return nil, moerr.NewInternalErrorf(
						ctx.GetContext(),
						"lists should be > 0.",
					)
				}
				alterTableReIndex.IndexAlgoParamList = opt.AlgoParamList
			case tree.INDEX_TYPE_HNSW:
				// PASS: keep options on change for incremental update
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					opt.KeyType.ToString(),
				)
			}

			name_not_found := true
			// check index
			for _, indexdef := range tableDef.Indexes {
				if constraintName == indexdef.IndexName {
					name_not_found = false
					break
				}
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't REINDEX '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterReindex{
					AlterReindex: alterTableReIndex,
				},
			}

		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"comment for field '%s' is too long",
					alterTable.TableDef.Name,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterComment{
					AlterComment: &plan.AlterTableComment{
						NewComment: opt.Comment,
					},
				},
			}

		case *tree.AlterOptionTableName:
			oldName := tableDef.Name
			newName := string(opt.Name.ToTableName().ObjectName)
			if oldName == newName {
				continue
			}

			// TODO ONLY Check
			_, tableDef, err := ctx.Resolve(databaseName, newName, nil)
			if err != nil {
				return nil, err
			}
			if tableDef != nil {
				return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
			}

			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterName{
					AlterName: &plan.AlterTableName{
						OldName: oldName,
						NewName: newName,
					},
				},
			}

			updateSqls = append(
				updateSqls,
				getSqlForRenameTable(databaseName, oldName, newName)...,
			)
		case *tree.AlterOptionAlterCheck, *tree.TableOptionCharset:
			continue

		case *tree.AlterTableModifyColumnClause:
			// defensively check again
			ok, _ := isInplaceModifyColumn(ctx.GetContext(), opt, tableDef)
			if !ok {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"failed inplace check: %s",
					formatTreeNode(opt),
				)
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			// update new column info to copy_table_def
			err := updateNewColumnInTableDef(
				ctx,
				alterTable.CopyTableDef,
				FindColumn(tableDef.Cols, opt.NewColumn.Name.ColName()),
				opt.NewColumn,
				opt.Position,
			)
			if err != nil {
				return nil, err
			}
		case *tree.AlterTableRenameColumnClause:
			if err := checkTableType(ctx.GetContext(), tableDef); err != nil {
				return nil, err
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			col := FindColumn(
				alterTable.CopyTableDef.Cols,
				opt.OldColumnName.ColName(),
			)
			if col == nil {
				return nil, moerr.NewBadFieldError(
					ctx.GetContext(),
					opt.OldColumnName.ColNameOrigin(),
					alterTable.TableDef.Name,
				)
			}
			oldColNameOrigin := col.OriginName
			newColNameOrigin := opt.NewColumnName.ColNameOrigin()

			if oldColNameOrigin == newColNameOrigin {
				continue
			}

			sqls, err := updateRenameColumnInTableDef(
				ctx,
				col,
				alterTable.CopyTableDef,
				opt,
			)
			if err != nil {
				return nil, err
			}

			updateSqls = append(updateSqls,
				getSqlForRenameColumn(tableDef.DbName,
					alterTable.TableDef.Name,
					oldColNameOrigin,
					newColNameOrigin)...)

			updateSqls = append(updateSqls, sqls...)

			alterTable.Actions = append(
				alterTable.Actions,
				&plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterRenameColumn{
						AlterRenameColumn: &plan.AlterRenameColumn{
							OldName:     oldColNameOrigin,
							NewName:     newColNameOrigin,
							SequenceNum: int32(col.Seqnum),
						},
					},
				},
			)

		default:
			return nil, moerr.NewInvalidInputf(
				ctx.GetContext(),
				unsupportedErrFmt,
				formatTreeNode(opt),
			)
		}
	}

	if alterTable.CopyTableDef != nil {
		alterTable.Actions = append(alterTable.Actions, &plan.AlterTable_Action{
			Action: &plan.AlterTable_Action_AlterReplaceDef{
				AlterReplaceDef: &plan.AlterReplaceDef{},
			},
		})
	}

	if stmt.PartitionOption != nil {
		// TODO: reimplement partition
		return nil, moerr.NewNotSupportedf(
			ctx.GetContext(),
			unsupportedErrFmt,
			formatTreeNode(stmt.PartitionOption),
		)
	}

	// check Constraint Name (include index/ unique)
	if err := checkConstraintNames(
		uniqueIndexInfos,
		secondaryIndexInfos,
		ctx.GetContext(),
	); err != nil {
		return nil, err
	}

	alterTable.DetectSqls = detectSqls
	alterTable.UpdateFkSqls = updateSqls
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_TABLE,
				Definition: &plan.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}, nil
}

func buildLockTables(stmt *tree.LockTableStmt, ctx CompilerContext) (*Plan, error) {
	lockTables := make([]*plan.TableLockInfo, 0, len(stmt.TableLocks))
	uniqueTableName := make(map[string]bool)

	//Check table locks
	for _, tableLock := range stmt.TableLocks {
		tb := tableLock.Table

		//get table name
		tblName := string(tb.ObjectName)

		// get database name
		var schemaName string
		if len(tb.SchemaName) == 0 {
			schemaName = ctx.DefaultDatabase()
		} else {
			schemaName = string(tb.SchemaName)
		}

		//check table whether exist
		obj, tableDef, err := ctx.Resolve(schemaName, tblName, nil)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tblName)
		}

		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot lock table in subscription database")
		}

		// check the stmt whether locks the same table
		if _, ok := uniqueTableName[tblName]; ok {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Not unique table %s", tblName)
		}

		uniqueTableName[tblName] = true

		tableLockInfo := &plan.TableLockInfo{
			LockType: plan.TableLockType(tableLock.LockType),
			TableDef: tableDef,
		}
		lockTables = append(lockTables, tableLockInfo)
	}

	LockTables := &plan.LockTables{
		TableLocks: lockTables,
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_LOCK_TABLES,
				Definition: &plan.DataDefinition_LockTables{
					LockTables: LockTables,
				},
			},
		},
	}, nil
}

func buildUnLockTables(stmt *tree.UnLockTableStmt, ctx CompilerContext) (*Plan, error) {
	unLockTables := &plan.UnLockTables{}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_UNLOCK_TABLES,
				Definition: &plan.DataDefinition_UnlockTables{
					UnlockTables: unLockTables,
				},
			},
		},
	}, nil
}

type FkData struct {
	// fk reference to itself
	IsSelfRefer bool
	// the database that the fk refers to
	ParentDbName string
	// the table that the fk refers to
	ParentTableName string
	//the columns in foreign key
	Cols *plan.FkColName
	// the columns referred
	ColsReferred *plan.FkColName
	//fk definition
	Def *plan.ForeignKeyDef
	//the column typs in foreign key
	ColTyps map[int]*plan.Type
	// update foreign keys relations
	UpdateSql string
	// forward reference
	ForwardRefer bool
}

// getForeignKeyData prepares the foreign key data.
// for fk refer except the self refer, it is same as the previous one.
// but for fk self refer, it is different in not checking fk self refer instantly.
// because it is not ready. It should be checked after the pk,uk has been ready.
func getForeignKeyData(ctx CompilerContext, dbName string, tableDef *TableDef, def *tree.ForeignKey) (*FkData, error) {
	refer := def.Refer
	fkData := FkData{
		Def: &plan.ForeignKeyDef{
			Name:        def.ConstraintSymbol,
			Cols:        make([]uint64, len(def.KeyParts)),
			OnDelete:    getRefAction(refer.OnDelete),
			OnUpdate:    getRefAction(refer.OnUpdate),
			ForeignCols: make([]uint64, len(refer.KeyParts)),
		},
	}

	// get fk columns of create table
	fkData.Cols = &plan.FkColName{
		Cols: make([]string, len(def.KeyParts)),
	}
	fkData.ColTyps = make(map[int]*plan.Type)
	name2ColDef := make(map[string]*ColDef)
	for _, colDef := range tableDef.Cols {
		name2ColDef[colDef.Name] = colDef
	}
	//get the column (id,name,type) from tableDef for the foreign key
	for i, keyPart := range def.KeyParts {
		colName := keyPart.ColName.ColName()
		if colDef, has := name2ColDef[colName]; has {
			//column id from tableDef
			fkData.Def.Cols[i] = colDef.ColId
			//column name from tableDef
			fkData.Cols.Cols[i] = colDef.Name
			//column type from tableDef
			fkData.ColTyps[i] = &colDef.Typ
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in the creating table '%v'", keyPart.ColName.ColNameOrigin(), tableDef.Name)
		}
	}

	fkData.ColsReferred = &plan.FkColName{
		Cols: make([]string, len(refer.KeyParts)),
	}
	for i, part := range refer.KeyParts {
		fkData.ColsReferred.Cols[i] = part.ColName.ColName()
	}

	// get foreign table & their columns
	parentTableName := string(refer.TableName.ObjectName)
	parentDbName := string(refer.TableName.SchemaName)
	if parentDbName == "" {
		parentDbName = ctx.DefaultDatabase()
	}

	if IsFkBannedDatabase(parentDbName) {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not refer foreign keys in %s", parentDbName)
	}

	//foreign key reference to itself
	if IsFkSelfRefer(parentDbName, parentTableName, dbName, tableDef.Name) {
		//should be handled later for fk self reference
		//PK and unique key may not be processed now
		//check fk columns can not reference to themselves
		//In self refer, the parent table is the table itself
		parentColumnsMap := make(map[string]int8)
		for _, part := range refer.KeyParts {
			parentColumnsMap[part.ColName.ColName()] = 0
		}
		for _, name := range fkData.Cols.Cols {
			if _, ok := parentColumnsMap[name]; ok {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "foreign key %s can not reference to itself", name)
			}
		}
		//for fk self refer. column id may be not ready.
		fkData.IsSelfRefer = true
		fkData.ParentDbName = parentDbName
		fkData.ParentTableName = parentTableName
		fkData.Def.ForeignTbl = 0
		fkData.UpdateSql = getSqlForAddFk(dbName, tableDef.Name, &fkData)
		return &fkData, nil
	}

	fkData.ParentDbName = parentDbName
	fkData.ParentTableName = parentTableName

	//make insert mo_foreign_keys
	fkData.UpdateSql = getSqlForAddFk(dbName, tableDef.Name, &fkData)

	_, parentTableDef, err := ctx.Resolve(parentDbName, parentTableName, nil)
	if err != nil {
		return nil, err
	}
	if parentTableDef == nil {
		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if !enabled {
			fkData.ForwardRefer = true
			return &fkData, nil
		}
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), ctx.DefaultDatabase(), parentTableName)
	}

	if parentTableDef.IsTemporary {
		return nil, moerr.NewNYI(ctx.GetContext(), "add foreign key for temporary table")
	}

	fkData.Def.ForeignTbl = parentTableDef.TblId

	//separate the rest of the logic in previous version
	//into an independent function checkFkColsAreValid
	//for reusing it in fk self refer that checks the
	//columns in fk definition are valid or not.
	if err := checkFkColsAreValid(ctx, &fkData, parentTableDef); err != nil {
		return nil, err
	}

	return &fkData, nil
}

/*
checkFkColsAreValid check foreign key columns is valid or not, then it saves them.
the columns referred by the foreign key in the children table must appear in the unique keys or primary key
in the parent table.

For instance:
create table f1 (a int ,b int, c int ,d int ,e int,

	primary key(a,b),  unique key(c,d), unique key (e))

Case 1:

	single column like "a" ,"b", "c", "d", "e" can be used as the column in foreign key of the child table
	due to they are the member of the primary key or some Unique key.

Case 2:

	"a, b" can be used as the columns in the foreign key of the child table
	due to they are the member of the primary key.

	"c, d" can be used as the columns in the foreign key of the child table
	due to they are the member of some unique key.

Case 3:

	"a, c" can not be used due to they belong to the different primary key / unique key
*/
func checkFkColsAreValid(ctx CompilerContext, fkData *FkData, parentTableDef *TableDef) error {
	//colId in parent table-> position in parent table
	columnIdPos := make(map[uint64]int)
	//columnName in parent table -> position in parent table
	columnNamePos := make(map[string]int)
	//columnName of index and pk of parent table -> colId in parent table
	uniqueColumns := make([]map[string]uint64, 0, len(parentTableDef.Cols))

	//1. collect parent column info
	for i, col := range parentTableDef.Cols {
		columnIdPos[col.ColId] = i
		columnNamePos[col.Name] = i
	}

	//2. check if the referred column does not exist in the parent table
	for _, colName := range fkData.ColsReferred.Cols {
		if _, exists := columnNamePos[colName]; !exists { // column exists in parent table
			return moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in table '%v'", colName, fkData.ParentTableName)
		}
	}

	//columnName in uk or pk -> its colId in the parent table
	collectIndexColumn := func(names []string) {
		ret := make(map[string]uint64)
		//columnName -> its colId in the parent table
		for _, colName := range names {
			ret[colName] = parentTableDef.Cols[columnNamePos[colName]].ColId
		}
		uniqueColumns = append(uniqueColumns, ret)
	}

	//3. collect pk column info of the parent table
	if parentTableDef.Pkey != nil {
		collectIndexColumn(parentTableDef.Pkey.Names)
	}

	//4. collect index column info of the parent table
	//secondary key?
	// now tableRef.Indices are empty, you can not test it
	for _, index := range parentTableDef.Indexes {
		if index.Unique {
			collectIndexColumn(index.Parts)
		}
	}

	//5. check if there is at least one unique key or primary key should have
	//the columns referenced by the foreign keys in the children tables.
	matchCol := make([]uint64, 0, len(fkData.ColsReferred.Cols))
	//iterate on every pk or uk
	for _, uniqueColumn := range uniqueColumns {
		//iterate on the referred column of fk
		for i, colName := range fkData.ColsReferred.Cols {
			//check if the referred column exists in this pk or uk
			if colId, ok := uniqueColumn[colName]; ok {
				// check column type
				// left part of expr: column type in parent table
				// right part of expr: column type in child table
				if parentTableDef.Cols[columnIdPos[colId]].Typ.Id != fkData.ColTyps[i].Id {
					return moerr.NewInternalErrorf(ctx.GetContext(), "type of reference column '%v' is not match for column '%v'", colName, fkData.Cols.Cols[i])
				}
				matchCol = append(matchCol, colId)
			} else {
				// column in fk does not exist in this pk or uk
				matchCol = matchCol[:0]
				break
			}
		}

		if len(matchCol) > 0 {
			break
		}
	}

	if len(matchCol) == 0 {
		return moerr.NewInternalError(ctx.GetContext(), "failed to add the foreign key constraint")
	} else {
		fkData.Def.ForeignCols = matchCol
	}
	return nil
}

// buildFkDataOfForwardRefer rebuilds the fk relationships based on
// the mo_catalog.mo_foreign_keys.
func buildFkDataOfForwardRefer(ctx CompilerContext,
	constraintName string,
	fkDefs []*FkReferDef,
	createTable *plan.CreateTable) (*FkData, error) {
	fkData := FkData{
		Def: &plan.ForeignKeyDef{
			Name:        constraintName,
			Cols:        make([]uint64, len(fkDefs)),
			OnDelete:    convertIntoReferAction(fkDefs[0].OnDelete),
			OnUpdate:    convertIntoReferAction(fkDefs[0].OnUpdate),
			ForeignCols: make([]uint64, len(fkDefs)),
		},
	}
	//1. get tableDef of the child table
	_, childTableDef, err := ctx.Resolve(fkDefs[0].Db, fkDefs[0].Tbl, nil)
	if err != nil {
		return nil, err
	}
	if childTableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), fkDefs[0].Db, fkDefs[0].Tbl)
	}
	//2. fill fkdata
	fkData.Cols = &plan.FkColName{
		Cols: make([]string, len(fkDefs)),
	}
	fkData.ColTyps = make(map[int]*plan.Type)

	name2ColDef := make(map[string]*ColDef)
	for _, def := range childTableDef.Cols {
		name2ColDef[def.Name] = def
	}
	for i, fkDef := range fkDefs {
		if colDef, has := name2ColDef[fkDef.Col]; has {
			//column id from tableDef
			fkData.Def.Cols[i] = colDef.ColId
			//column name from tableDef
			fkData.Cols.Cols[i] = colDef.Name
			//column type from tableDef
			fkData.ColTyps[i] = &colDef.Typ
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in table '%v'", fkDef.Col, fkDefs[0].Tbl)
		}
	}

	fkData.ColsReferred = &plan.FkColName{
		Cols: make([]string, len(fkDefs)),
	}
	for i, def := range fkDefs {
		fkData.ColsReferred.Cols[i] = def.ReferCol
	}

	//3. check fk valid or not
	if err := checkFkColsAreValid(ctx, &fkData, createTable.TableDef); err != nil {
		return nil, err
	}
	return &fkData, nil
}

func getAutoIncrementOffsetFromVariables(ctx CompilerContext) (uint64, bool) {
	v, err := ctx.ResolveVariable("auto_increment_offset", true, false)
	if err == nil {
		if offset, ok := v.(int64); ok && offset > 1 {
			return uint64(offset - 1), true
		}
	}
	return 0, false
}

var unitDurations = map[string]time.Duration{
	"second": time.Second,
	"minute": time.Minute,
	"hour":   time.Hour,
	"day":    time.Hour * 24,
	"week":   time.Hour * 24 * 7,
	"month":  time.Hour * 24 * 30,
}

func parseDuration(ctx context.Context, period uint64, unit string) (time.Duration, error) {
	unitDuration, ok := unitDurations[strings.ToLower(unit)]
	if !ok {
		return 0, moerr.NewInvalidArg(ctx, "time unit", unit)
	}
	seconds := period * uint64(unitDuration)
	return time.Duration(seconds), nil
}

func buildCreatePitr(stmt *tree.CreatePitr, ctx CompilerContext) (*Plan, error) {
	// only sys can create cluster level pitr
	currentAccount := ctx.GetAccountName()
	currentAccountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if stmt.Level == tree.PITRLEVELCLUSTER && currentAccount != "sys" {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only sys tenant can create cluster level pitr")
	}

	// only sys can create tenant level pitr for other tenant
	if stmt.Level == tree.PITRLEVELACCOUNT {
		if len(stmt.AccountName) > 0 && currentAccount != "sys" {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only sys tenant can create tenant level pitr for other tenant")
		}
	}

	// Check PITR value range
	pitrVal := stmt.PitrValue
	if pitrVal <= 0 || pitrVal > 100 {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid pitr value %d", pitrVal)
	}

	// Check if PITR unit is valid
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid pitr unit %s", pitrUnit)
	}

	// check pitr exists or not
	if string(stmt.Name) == SYSMOCATALOGPITR {
		return nil, moerr.NewInternalError(ctx.GetContext(), "pitr name is reserved")
	}

	// Validate related objects according to PITR level
	var databaseId uint64
	var tableId uint64
	accountId := currentAccountId
	accountName := currentAccount
	switch stmt.Level {
	case tree.PITRLEVELACCOUNT:
		if len(stmt.AccountName) > 0 {
			accountIds, err := ctx.ResolveAccountIds([]string{string(stmt.AccountName)})
			if err != nil {
				return nil, err
			}
			if len(accountIds) == 0 {
				return nil, moerr.NewInternalError(ctx.GetContext(), "account "+string(stmt.AccountName)+" does not exist")
			}
			accountId = accountIds[len(accountIds)-1]
			accountName = string(stmt.AccountName)
		}
	case tree.PITRLEVELDATABASE:
		if !ctx.DatabaseExists(string(stmt.DatabaseName), nil) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "database "+string(stmt.DatabaseName)+" does not exist")
		}
		databaseId, err = ctx.GetDatabaseId(string(stmt.DatabaseName), nil)
		if err != nil {
			return nil, err
		}
	case tree.PITRLEVELTABLE:
		if !ctx.DatabaseExists(string(stmt.DatabaseName), nil) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "database "+string(stmt.DatabaseName)+" does not exist")
		}
		objRef, tableDef, err := ctx.Resolve(string(stmt.DatabaseName), string(stmt.TableName), nil)
		if err != nil {
			return nil, err
		}
		if objRef == nil || tableDef == nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "table "+string(stmt.DatabaseName)+"."+string(stmt.TableName)+" does not exist")
		}
		tableId = tableDef.TblId
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_PITR,
				Definition: &plan.DataDefinition_CreatePitr{
					CreatePitr: &plan.CreatePitr{
						IfNotExists:       stmt.IfNotExists,
						Name:              string(stmt.Name),
						Level:             int32(stmt.Level),
						AccountName:       accountName,
						DatabaseName:      string(stmt.DatabaseName),
						TableName:         string(stmt.TableName),
						PitrValue:         stmt.PitrValue,
						PitrUnit:          stmt.PitrUnit,
						DatabaseId:        databaseId,
						TableId:           tableId,
						AccountId:         accountId,
						CurrentAccountId:  currentAccountId,
						CurrentAccount:    currentAccount,
						OriginAccountName: len(stmt.AccountName) > 0,
					},
				},
			},
		},
	}, nil
}

func buildDropPitr(stmt *tree.DropPitr, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_DROP_PITR
	// Remove privilege check, no account ID validation

	// Build drop pitr plan
	dropPitr := &plan.DropPitr{
		IfExists: stmt.IfExists,
		Name:     string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: ddlType,
				Definition: &plan.DataDefinition_DropPitr{
					DropPitr: dropPitr,
				},
			},
		},
	}, nil
}
