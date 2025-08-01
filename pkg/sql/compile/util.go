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

package compile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	INDEX_TYPE_PRIMARY  = "PRIMARY"
	INDEX_TYPE_UNIQUE   = "UNIQUE"
	INDEX_TYPE_MULTIPLE = "MULTIPLE"
	INDEX_TYPE_FULLTEXT = "FULLTEXT"
	INDEX_TYPE_SPATIAL  = "SPATIAL"
)

const (
	INDEX_VISIBLE_YES = 1
	INDEX_VISIBLE_NO  = 0
)

const (
	INDEX_HIDDEN_YES = 1
	INDEX_HIDDEN_NO  = 0
)

const (
	NULL_VALUE   = "null"
	EMPTY_STRING = ""
)

const (
	ALLOCID_INDEX_KEY = "index_key"
)

var (
	needSkipDbs = map[string]bool{
		"mysql":              true,
		"system":             true,
		"system_metrics":     true,
		"mo_task":            true,
		"mo_debug":           true,
		"information_schema": true,
		catalog.MO_CATALOG:   true,
	}
)
var (
	// see the comment in fuzzyCheck func genCondition for the reason why has to be two SQLs
	fuzzyNonCompoundCheck = "select %s from `%s`.`%s` where %s in (%s) group by %s having count(*) > 1 limit 1;"
	fuzzyCompoundCheck    = "select serial(%s) from `%s`.`%s` where %s group by serial(%s) having count(*) > 1 limit 1;"
)

var (
	insertIntoSingleIndexTableWithPKeyFormat    = "insert into  `%s`.`%s` select (%s), %s from `%s`.`%s` where (%s) is not null;"
	insertIntoUniqueIndexTableWithPKeyFormat    = "insert into  `%s`.`%s` select serial(%s), %s from `%s`.`%s` where serial(%s) is not null;"
	insertIntoSecondaryIndexTableWithPKeyFormat = "insert into  `%s`.`%s` select serial_full(%s), %s from `%s`.`%s`;"
	insertIntoSingleIndexTableWithoutPKeyFormat = "insert into  `%s`.`%s` select (%s) from `%s`.`%s` where (%s) is not null;"
	insertIntoIndexTableWithoutPKeyFormat       = "insert into  `%s`.`%s` select serial(%s) from `%s`.`%s` where serial(%s) is not null;"
	insertIntoMasterIndexTableFormat            = "insert into  `%s`.`%s` select serial_full('%s', %s, %s), %s from `%s`.`%s`;"
)

var (
	deleteMoIndexesWithDatabaseIdFormat          = `delete from mo_catalog.mo_indexes where database_id = %v;`
	deleteMoIndexesWithTableIdFormat             = `delete from mo_catalog.mo_indexes where table_id = %v;`
	deleteMoIndexesWithTableIdAndIndexNameFormat = `delete from mo_catalog.mo_indexes where table_id = %v and name = '%s';`
	updateMoIndexesVisibleFormat                 = `update mo_catalog.mo_indexes set is_visible = %v where table_id = %v and name = '%s';`
	updateMoIndexesTruncateTableFormat           = `update mo_catalog.mo_indexes set table_id = %v where table_id = %v`
	updateMoIndexesAlgoParams                    = `update mo_catalog.mo_indexes set algo_params = '%s' where table_id = %v and name = '%s';`
	updateMoMergeSettings                        = `update mo_catalog.mo_merge_settings set tid = %v where account_id = %v and tid = %v;`
)

var (
	dropTableBeforeDropDatabase = "drop table if exists `%v`.`%v`;"
)

var (
	insertIntoFullTextIndexTableFormat = "INSERT INTO `%s`.`%s` SELECT f.* FROM `%s`.`%s` AS %s CROSS APPLY fulltext_index_tokenize('%s', %s, %s) AS f;"
)

var (
	insertIntoHnswIndexTableFormat = "SELECT f.* from `%s`.`%s` AS %s CROSS APPLY hnsw_create('%s', '%s', %s, %s) AS f;"
)

// genInsertIndexTableSql: Generate an insert statement for inserting data into the index table
func genInsertIndexTableSql(originTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string, isUnique bool) string {
	// insert data into index table
	var insertSQL string
	temp := partsToColsStr(indexDef.Parts)
	if len(originTableDef.Pkey.PkeyColName) == 0 {
		if len(indexDef.Parts) == 1 {
			insertSQL = fmt.Sprintf(insertIntoSingleIndexTableWithoutPKeyFormat, DBName, indexDef.IndexTableName, temp, DBName, originTableDef.Name, temp)
		} else {
			insertSQL = fmt.Sprintf(insertIntoIndexTableWithoutPKeyFormat, DBName, indexDef.IndexTableName, temp, DBName, originTableDef.Name, temp)
		}
	} else {
		pkeyName := originTableDef.Pkey.PkeyColName
		var pKeyMsg string
		if pkeyName == catalog.CPrimaryKeyColName {
			pKeyMsg = "serial("
			for i, part := range originTableDef.Pkey.Names {
				if i == 0 {
					pKeyMsg += "`" + part + "`"
				} else {
					pKeyMsg += "," + "`" + part + "`"
				}
			}
			pKeyMsg += ")"
		} else {
			pKeyMsg = pkeyName
		}
		if len(indexDef.Parts) == 1 {
			insertSQL = fmt.Sprintf(insertIntoSingleIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name, temp)
		} else {
			if isUnique {
				insertSQL = fmt.Sprintf(insertIntoUniqueIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name, temp)
			} else {
				insertSQL = fmt.Sprintf(insertIntoSecondaryIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name)
			}
		}
	}
	return insertSQL
}

// genInsertIndexTableSqlForMasterIndex: Create inserts for master index table
func genInsertIndexTableSqlForMasterIndex(originTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string) []string {
	// insert data into index table
	var insertSQLs = make([]string, len(indexDef.Parts))

	pkeyName := originTableDef.Pkey.PkeyColName
	var pKeyMsg string
	if pkeyName == catalog.CPrimaryKeyColName {
		pKeyMsg = "serial("
		for i, part := range originTableDef.Pkey.Names {
			if i == 0 {
				pKeyMsg += part
			} else {
				pKeyMsg += "," + part
			}
		}
		pKeyMsg += ")"
	} else {
		pKeyMsg = pkeyName
	}

	colSeqNumMap := make(map[string]string)
	for _, col := range originTableDef.Cols {
		// NOTE:
		// ColDef.ColId is not used as "after alter table, different columns may have the same colId"
		// ColDef.SeqNum is used instead as it is always unique.
		colSeqNumMap[col.GetName()] = fmt.Sprintf("%d", col.GetSeqnum())
	}

	for i, part := range indexDef.Parts {
		insertSQLs[i] = fmt.Sprintf(insertIntoMasterIndexTableFormat,
			DBName, indexDef.IndexTableName,
			colSeqNumMap[part], part, pKeyMsg, pKeyMsg, DBName, originTableDef.Name)
	}

	return insertSQLs
}

// genInsertMOIndexesSql: Generate an insert statement for insert index metadata into `mo_catalog.mo_indexes`
func genInsertMOIndexesSql(eg engine.Engine, proc *process.Process, databaseId string, tableId uint64, ct *engine.ConstraintDef, tableDef *plan.TableDef) (string, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1024))
	buffer.WriteString("insert into mo_catalog.mo_indexes values")

	getOriginName := func(name string) string {
		if idx, ok := tableDef.Name2ColIndex[name]; ok {
			return tableDef.Cols[idx].OriginName
		}
		return name
	}

	isFirst := true
	for _, constraint := range ct.Cts {
		switch def := constraint.(type) {
		case *engine.IndexDef:
			for _, indexDef := range def.Indexes {
				ctx, cancelFunc := context.WithTimeoutCause(proc.Ctx, time.Second*30, moerr.CauseGenInsertMOIndexesSql)
				indexId, err := eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
				cancelFunc()
				if err != nil {
					return "", moerr.AttachCause(ctx, err)
				}

				for i, part := range indexDef.Parts {
					// NOTE: Don't resolve the alias here.
					// If we resolve it here, it will insert "OriginalPKColumnName" into the "mo_catalog.mo_indexes" table instead of
					// "AliasPKColumnName". This will result is issues filter "Programmatically added PK" from the output of
					// "show indexes" and "show create table" command.

					//1. index id
					if isFirst {
						fmt.Fprintf(buffer, "(%d, ", indexId)
						isFirst = false
					} else {
						fmt.Fprintf(buffer, ", (%d, ", indexId)
					}

					//2. table_id
					fmt.Fprintf(buffer, "%d, ", tableId)

					// 3. databaseId
					fmt.Fprintf(buffer, "%s, ", databaseId)

					// 4.index.IndexName
					fmt.Fprintf(buffer, "'%s', ", indexDef.IndexName)

					// 5. index_type
					var index_type string
					if indexDef.Unique {
						index_type = INDEX_TYPE_UNIQUE
					} else {
						index_type = INDEX_TYPE_MULTIPLE
					}
					fmt.Fprintf(buffer, "'%s', ", index_type)

					//6. algorithm
					var algorithm = indexDef.IndexAlgo
					fmt.Fprintf(buffer, "'%s', ", algorithm)

					//7. algorithm_table_type
					var algorithm_table_type = indexDef.IndexAlgoTableType
					fmt.Fprintf(buffer, "'%s', ", algorithm_table_type)

					//8. algorithm_params
					var algorithm_params = indexDef.IndexAlgoParams
					fmt.Fprintf(buffer, "'%s', ", algorithm_params)

					// 9. index visible
					fmt.Fprintf(buffer, "%d, ", INDEX_VISIBLE_YES)

					// 10. index vec_hidden
					fmt.Fprintf(buffer, "%d, ", INDEX_HIDDEN_NO)

					// 11. index vec_comment
					fmt.Fprintf(buffer, "'%s', ", indexDef.Comment)

					// 12. index vec_column_name
					fmt.Fprintf(buffer, "'%s', ", getOriginName(part))

					// 13. index vec_ordinal_position
					fmt.Fprintf(buffer, "%d, ", i+1)

					// 14. index vec_options
					if indexDef.Option != nil {
						if indexDef.Option.ParserName != "" {
							fmt.Fprintf(buffer, "'parser=%s,ngram_token_size=%d', ", indexDef.Option.ParserName, indexDef.Option.NgramTokenSize)
						}
					} else {
						fmt.Fprintf(buffer, "%s, ", NULL_VALUE)
					}

					// 15. index vec_index_table
					if indexDef.TableExist {
						fmt.Fprintf(buffer, "'%s')", indexDef.IndexTableName)
					} else {
						fmt.Fprintf(buffer, "%s)", NULL_VALUE)
					}
				}
			}
		case *engine.PrimaryKeyDef:
			ctx, cancelFunc := context.WithTimeoutCause(proc.Ctx, time.Second*30, moerr.CauseGenInsertMOIndexesSql2)
			index_id, err := eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
			cancelFunc()
			if err != nil {
				return "", moerr.AttachCause(ctx, err)
			}
			if def.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				for i, colName := range def.Pkey.Names {
					//1. index id
					if isFirst {
						fmt.Fprintf(buffer, "(%d, ", index_id)
						isFirst = false
					} else {
						fmt.Fprintf(buffer, ", (%d, ", index_id)
					}

					//2. table_id
					fmt.Fprintf(buffer, "%d, ", tableId)

					// 3. databaseId
					fmt.Fprintf(buffer, "%s, ", databaseId)

					// 4.index.IndexName
					fmt.Fprintf(buffer, "'%s', ", "PRIMARY")

					// 5.index_type
					fmt.Fprintf(buffer, "'%s', ", INDEX_TYPE_PRIMARY)

					//6. algorithm
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					//7. algorithm_table_type
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					//8. algorithm_params
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					//9. index visible
					fmt.Fprintf(buffer, "%d, ", INDEX_VISIBLE_YES)

					// 10. index vec_hidden
					fmt.Fprintf(buffer, "%d, ", INDEX_HIDDEN_NO)

					// 11. index vec_comment
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					// 12. index vec_column_name
					fmt.Fprintf(buffer, "'%s', ", getOriginName(colName))

					// 13. index vec_ordinal_position
					fmt.Fprintf(buffer, "%d, ", i+1)

					// 14. index vec_options
					fmt.Fprintf(buffer, "%s, ", NULL_VALUE)

					// 15. index vec_index_table
					fmt.Fprintf(buffer, "%s)", NULL_VALUE)
				}
			}
		}
	}
	buffer.WriteString(";")
	return buffer.String(), nil
}

// makeInsertSingleIndexSQL: make index metadata information sql for a single index object
func makeInsertSingleIndexSQL(eg engine.Engine, proc *process.Process, databaseId string, tableId uint64, idxdef *plan.IndexDef, tableDef *plan.TableDef) (string, error) {
	if idxdef == nil {
		return "", nil
	}
	ct := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.IndexDef{
				Indexes: []*plan.IndexDef{idxdef},
			},
		},
	}
	insertMoIndexesSql, err := genInsertMOIndexesSql(eg, proc, databaseId, tableId, ct, tableDef)
	if err != nil {
		return "", err
	}
	return insertMoIndexesSql, nil
}

// makeInsertMultiIndexSQL :Synchronize the index metadata information of the table to the index metadata table
func makeInsertMultiIndexSQL(eg engine.Engine, ctx context.Context, proc *process.Process, dbSource engine.Database, relation engine.Relation) (string, error) {
	if dbSource == nil || relation == nil {
		return "", nil
	}
	databaseId := dbSource.GetDatabaseId(ctx)
	tableId := relation.GetTableID(ctx)
	tableDef := relation.GetTableDef(ctx)

	ct, err := GetConstraintDef(ctx, relation)
	if err != nil {
		return "", err
	}
	if ct == nil {
		return "", nil
	}

	hasIndex := false
	for _, constraint := range ct.Cts {
		if idxdef, ok := constraint.(*engine.IndexDef); ok && len(idxdef.Indexes) > 0 {
			hasIndex = true
			break
		}
		if pkdef, ok := constraint.(*engine.PrimaryKeyDef); ok {
			if pkdef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				hasIndex = true
				break
			}
		}
	}
	if !hasIndex {
		return "", nil
	}

	insertMoIndexesSql, err := genInsertMOIndexesSql(eg, proc, databaseId, tableId, ct, tableDef)
	if err != nil {
		return "", err
	}
	return insertMoIndexesSql, nil
}

func (s *Scope) checkTableWithValidIndexes(c *Compile, relation engine.Relation) error {
	if relation == nil {
		return nil
	}

	ct, err := GetConstraintDef(c.proc.Ctx, relation)
	if err != nil {
		return err
	}
	if ct == nil {
		return nil
	}

	for _, constraint := range ct.Cts {
		if idxdef, ok := constraint.(*engine.IndexDef); ok && len(idxdef.Indexes) > 0 {
			for _, idx := range idxdef.Indexes {
				if idx.TableExist {
					var indexflag string
					if catalog.IsHnswIndexAlgo(idx.IndexAlgo) {
						indexflag = hnswIndexFlag
					} else if catalog.IsIvfIndexAlgo(idx.IndexAlgo) {
						indexflag = ivfFlatIndexFlag
					} else if catalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
						indexflag = fulltextIndexFlag
					}

					if len(indexflag) > 0 {
						if ok, err := s.isExperimentalEnabled(c, indexflag); err != nil {
							return err
						} else if !ok {
							return moerr.NewInternalError(c.proc.Ctx, fmt.Sprintf("%s is not enabled", indexflag))
						}
					}
				}

			}

			break
		}
	}

	return nil
}

func partsToColsStr(parts []string) string {
	var temp string
	for i, part := range parts {
		part = catalog.ResolveAlias(part)
		if i == 0 {
			temp += part
		} else {
			temp += "," + part
		}
	}
	return temp
}

// haveSinkScanInPlan Start from the `curNodeIdx` node, recursively check its Subtree all nodes,
// determine if they contain `SINK_SCAN` node in the subtree
func haveSinkScanInPlan(nodes []*plan.Node, curNodeIdx int32) bool {
	node := nodes[curNodeIdx]
	if node.NodeType == plan.Node_SINK_SCAN {
		return true
	}
	for _, newIdx := range node.Children {
		flag := haveSinkScanInPlan(nodes, newIdx)
		if flag {
			return flag
		}
	}
	return false
}

var GetConstraintDef = func(ctx context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
	defs, err := rel.TableDefs(ctx)
	if err != nil {
		return nil, err
	}

	return GetConstraintDefFromTableDefs(defs), nil
}

func GetConstraintDefFromTableDefs(defs []engine.TableDef) *engine.ConstraintDef {
	var cstrDef *engine.ConstraintDef
	for _, def := range defs {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			cstrDef = ct
			break
		}
	}
	if cstrDef == nil {
		cstrDef = &engine.ConstraintDef{}
		cstrDef.Cts = make([]engine.Constraint, 0)
	}
	return cstrDef
}

func genInsertIndexTableSqlForFullTextIndex(originalTableDef *plan.TableDef, indexDef *plan.IndexDef, qryDatabase string) []string {
	src_alias := "src"
	pkColName := src_alias + "." + originalTableDef.Pkey.PkeyColName
	params := indexDef.IndexAlgoParams
	tblname := indexDef.IndexTableName

	parts := make([]string, 0, len(indexDef.Parts))
	for _, p := range indexDef.Parts {
		parts = append(parts, src_alias+"."+p)
	}

	concat := strings.Join(parts, ",")

	sql := fmt.Sprintf(insertIntoFullTextIndexTableFormat,
		qryDatabase, tblname,
		qryDatabase, originalTableDef.Name,
		src_alias,
		params,
		pkColName,
		concat)

	return []string{sql}
}

func genDeleteHnswIndex(proc *process.Process, indexDefs map[string]*plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef) ([]string, error) {
	idxdef_meta, ok := indexDefs[catalog.Hnsw_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_meta index definition not found")
	}

	idxdef_index, ok := indexDefs[catalog.Hnsw_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_index index definition not found")
	}

	sqls := make([]string, 0, 2)

	sql := fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, idxdef_meta.IndexTableName)
	sqls = append(sqls, sql)
	sql = fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, idxdef_index.IndexTableName)
	sqls = append(sqls, sql)

	return sqls, nil

}

func genBuildHnswIndex(proc *process.Process, indexDefs map[string]*plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef) ([]string, error) {
	var cfg vectorindex.IndexTableConfig
	src_alias := "src"
	pkColName := src_alias + "." + originalTableDef.Pkey.PkeyColName

	idxdef_meta, ok := indexDefs[catalog.Hnsw_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_meta index definition not found")
	}
	cfg.MetadataTable = idxdef_meta.IndexTableName

	idxdef_index, ok := indexDefs[catalog.Hnsw_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_index index definition not found")
	}
	cfg.IndexTable = idxdef_index.IndexTableName
	cfg.DbName = qryDatabase
	cfg.SrcTable = originalTableDef.Name
	cfg.PKey = pkColName
	cfg.KeyPart = idxdef_index.Parts[0]
	val, err := proc.GetResolveVariableFunc()("hnsw_threads_build", true, false)
	if err != nil {
		return nil, err
	}
	cfg.ThreadsBuild = val.(int64)

	idxcap, err := proc.GetResolveVariableFunc()("hnsw_max_index_capacity", true, false)
	if err != nil {
		return nil, err
	}
	cfg.IndexCapacity = idxcap.(int64)

	params := idxdef_index.IndexAlgoParams

	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	part := src_alias + "." + idxdef_index.Parts[0]

	sql := fmt.Sprintf(insertIntoHnswIndexTableFormat,
		qryDatabase, originalTableDef.Name,
		src_alias,
		params,
		string(cfgbytes),
		pkColName,
		part)

	return []string{sql}, nil
}
