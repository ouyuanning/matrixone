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

package engine

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func GetTempTableName(DbName string, TblName string) string {
	return strings.ReplaceAll(DbName, ".", "DOT") + "." + strings.ReplaceAll(TblName, ".", "DOT")
}

func (e *EntireEngine) New(ctx context.Context, op client.TxnOperator) error {
	err := e.Engine.New(ctx, op)
	if err == nil && e.TempEngine != nil {
		return e.TempEngine.New(ctx, op)
	}
	return err
}

func (e *EntireEngine) LatestLogtailAppliedTime() timestamp.Timestamp {
	return e.Engine.LatestLogtailAppliedTime()
}

func (e *EntireEngine) HasTempEngine() bool {
	return e.TempEngine != nil
}

func (e *EntireEngine) Delete(ctx context.Context, databaseName string, op client.TxnOperator) error {
	return e.Engine.Delete(ctx, databaseName, op)
}

func (e *EntireEngine) Create(ctx context.Context, databaseName string, op client.TxnOperator) error {
	return e.Engine.Create(ctx, databaseName, op)
}

func (e *EntireEngine) Databases(ctx context.Context, op client.TxnOperator) (databaseNames []string, err error) {
	return e.Engine.Databases(ctx, op)
}

func (e *EntireEngine) Database(ctx context.Context, databaseName string, op client.TxnOperator) (Database, error) {
	if databaseName == defines.TEMPORARY_DBNAME {
		if e.TempEngine != nil {
			return e.TempEngine.Database(ctx, defines.TEMPORARY_DBNAME, op)
		} else {
			return nil, moerr.NewInternalError(ctx, "temporary engine not init yet")
		}
	}
	return e.Engine.Database(ctx, databaseName, op)
}

func (e *EntireEngine) Nodes(
	isInternal bool, tenant string, username string, cnLabel map[string]string) (cnNodes Nodes, err error,
) {
	return e.Engine.Nodes(isInternal, tenant, username, cnLabel)
}

func (e *EntireEngine) Hints() Hints {
	return e.Engine.Hints()
}

func (e *EntireEngine) BuildBlockReaders(
	ctx context.Context,
	proc any,
	ts timestamp.Timestamp,
	expr *plan.Expr,
	def *plan.TableDef,
	relData RelData,
	num int) ([]Reader, error) {
	return e.Engine.BuildBlockReaders(ctx, proc, ts, expr, def, relData, num)
}

func (e *EntireEngine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	return e.Engine.GetNameById(ctx, op, tableId)
}

func (e *EntireEngine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel Relation, err error) {
	return e.Engine.GetRelationById(ctx, op, tableId)
}

func (e *EntireEngine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return e.Engine.AllocateIDByKey(ctx, key)
}

func (e *EntireEngine) TryToSubscribeTable(ctx context.Context, dbID, tbID uint64, dbName, tblName string) error {
	return e.Engine.TryToSubscribeTable(ctx, dbID, tbID, dbName, tblName)
}

func (e *EntireEngine) UnsubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return e.Engine.UnsubscribeTable(ctx, dbID, tbID)
}

func (e *EntireEngine) PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool {
	return e.Engine.PrefetchTableMeta(ctx, key)
}

func (e *EntireEngine) Stats(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	return e.Engine.Stats(ctx, key, sync)
}

func (e *EntireEngine) GetMessageCenter() any {
	return e.Engine.GetMessageCenter()
}

func (e *EntireEngine) GetService() string {
	return e.Engine.GetService()
}
