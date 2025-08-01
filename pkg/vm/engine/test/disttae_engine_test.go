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

package test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"

	"github.com/stretchr/testify/assert"
)

func Test_InsertRows(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	err = disttaeEngine.Engine.Create(ctx, databaseName, txn)
	require.Nil(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txn)
	require.Nil(t, err)

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName

	defs, err := testutil.EngineTableDefBySchema(schema)
	require.Nil(t, err)

	err = db.Create(ctx, tableName, defs)
	require.Nil(t, err)

	rel, err := db.Relation(ctx, tableName, nil)
	require.Nil(t, err)
	require.Contains(t, rel.GetTableName(), tableName)

	bat := catalog2.MockBatch(schema, 10)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.Nil(t, err)

	err = txn.Commit(ctx)
	require.Nil(t, err)

	err = disttaeEngine.SubscribeTable(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx), databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.Nil(t, err)

		fmt.Println(stats.String())

		require.Equal(t, 10, stats.InmemRows.VisibleCnt)
	}

	err = taeHandler.GetDB().FlushTable(ctx, accountId, rel.GetDBID(ctx), rel.GetTableID(ctx), types.TimestampToTS(disttaeEngine.Now()))
	require.Nil(t, err)
	// check partition state, after flush
	{

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.Nil(t, err)

		fmt.Println(stats.String())

		expect := testutil.PartitionStateStats{
			DataObjectsVisible:   testutil.PObjectStats{ObjCnt: 1, BlkCnt: 1, RowCnt: 10},
			DataObjectsInvisible: testutil.PObjectStats{ObjCnt: 1, BlkCnt: 1, RowCnt: 10},
			InmemRows:            testutil.PInmemRowsStats{},
			CheckpointCnt:        0,
		}

		require.Equal(t, expect, stats.Summary())

	}
}

// Create database and tables, and check the data length in the system tables in TN
func TestSystemDB1(t *testing.T) {
	p := testutil.InitEnginePack(testutil.TestOptions{}, t)
	defer p.Close()

	txnop := p.StartCNTxn()

	var err error
	p.CreateDB(txnop, "db1")

	schema := catalog2.MockSchema(2, 0)
	schema.Name = "test1inDb2"
	p.CreateDBAndTable(txnop, "db2", schema)

	dbs, err := p.D.Engine.Databases(p.Ctx, txnop)
	require.NoError(t, err)
	require.Equal(t, 2+1, len(dbs))
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	dbs, err = p.D.Engine.Databases(p.Ctx, txnop)
	require.NoError(t, err)
	txnop.GetWorkspace().StartStatement()
	require.Equal(t, 2+1, len(dbs))

	txn, err := p.T.StartTxn()
	require.NoError(t, err)

	catalogDB, err := txn.GetDatabase(catalog.MO_CATALOG)
	require.NoError(t, err)
	table, err := catalogDB.GetRelationByName(catalog.MO_DATABASE)
	require.NoError(t, err)
	it := table.MakeObjectIt(false)
	tableSchema := table.GetMeta().(*catalog2.TableEntry).GetLastestSchema(false)
	for it.Next() {
		blk := it.GetObject()
		var view *containers.Batch
		err := blk.Scan(p.Ctx, &view, 0, []int{
			tableSchema.GetColIdx(catalog.SystemDBAttr_Name)}, common.DefaultAllocator)
		require.Equal(t, 2, view.Length())
		require.Nil(t, err)
		view.Close()
		view = nil
		err = blk.Scan(p.Ctx, &view, 0, []int{
			tableSchema.GetColIdx(catalog.SystemDBAttr_Name)}, common.DefaultAllocator)
		require.Nil(t, err)
		require.Equal(t, 2, view.Length())
		view.Close()
	}

	table, err = catalogDB.GetRelationByName(catalog.MO_TABLES)
	require.Nil(t, err)
	it = table.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		var view *containers.Batch
		err := blk.Scan(p.Ctx, &view, 0, []int{
			tableSchema.GetColIdx(catalog.SystemDBAttr_Name)}, common.DefaultAllocator)
		require.Nil(t, err)
		require.Equal(t, 1, view.Length())
		view.Close()
		view = nil
		err = blk.Scan(p.Ctx, &view, 0, []int{
			tableSchema.GetColIdx(catalog.SystemDBAttr_Name)}, common.DefaultAllocator)
		require.NoError(t, err)
		defer view.Close()
		require.Equal(t, 1, view.Length())
	}

	table, err = catalogDB.GetRelationByName(catalog.MO_COLUMNS)
	require.Nil(t, err)
	tableSchema = table.GetMeta().(*catalog2.TableEntry).GetLastestSchema(false)
	var bat *containers.Batch
	it = table.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		err := blk.Scan(p.Ctx, &bat, 0, []int{
			tableSchema.GetColIdx(catalog.SystemColAttr_DBName),
			tableSchema.GetColIdx(catalog.SystemColAttr_RelName),
			tableSchema.GetColIdx(catalog.SystemColAttr_Name)}, common.DefaultAllocator)
		require.NoError(t, err)
	}
	require.Equal(t, 3, bat.Length())
	t.Log(bat.PPString(10))
	bat.Close()
}

type dummyCpkGetter struct{}

func (c *dummyCpkGetter) CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error) {
	return "", types.TS{}, nil
}

func (c *dummyCpkGetter) FlushTable(ctx context.Context, _ uint32, dbID, tableID uint64, ts types.TS) error {
	return nil
}

func totsp(ts types.TS) *timestamp.Timestamp {
	t := ts.ToTimestamp()
	return &t
}

func TestLogtailBasic(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	opts.LogtailCfg = &options.LogtailCfg{PageSize: 30}
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()
	logMgr := tae.LogtailMgr

	// at first, we can see nothing
	minTs, maxTs := types.BuildTS(0, 0), types.BuildTS(1000, 1000)
	reader := logMgr.GetReader(minTs, maxTs)
	require.False(t, reader.IsDirtyOnTable(1000, 1000))
	schema := catalog2.MockSchemaAll(2, -1)
	schema.Name = "test"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 1
	// craete 2 db and 2 tables
	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "todrop", schema)
	_, tH := p.CreateDBAndTable(txnop, "db", schema)
	dbID := tH.GetDBID(p.Ctx)
	tableID := tH.GetTableID(p.Ctx)
	require.NoError(t, txnop.Commit(p.Ctx))
	catalogWriteTs := txnop.Txn().CommitTS

	// drop the first db
	txnop = p.StartCNTxn()
	require.Nil(t, p.D.Engine.Delete(p.Ctx, "todrop", txnop))
	require.NoError(t, txnop.Commit(p.Ctx))
	catalogDropTs := txnop.Txn().CommitTS

	writeTs := make([]types.TS, 0, 120)
	deleteRowIDs := make([]types.Rowid, 0, 10)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		// insert 100 rows
		for i := 0; i < 100; i++ {
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			tbl, _ := db.GetRelationByName("test")
			tbl.Append(p.Ctx, catalog2.MockBatch(schema, 1))
			require.NoError(t, txn.Commit(p.Ctx))
			writeTs = append(writeTs, txn.GetPrepareTS())
		}
		// delete the row whose offset is 5 for every block
		{
			// collect rowid
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			tbl, _ := db.GetRelationByName("test")
			blkIt := tbl.MakeObjectIt(false)
			for blkIt.Next() {
				obj := blkIt.GetObject()
				id := obj.GetMeta().(*catalog2.ObjectEntry).ID()
				for j := 0; j < obj.BlkCnt(); j++ {
					blkID := objectio.NewBlockidWithObjectID(id, uint16(j))
					deleteRowIDs = append(deleteRowIDs, objectio.NewRowid(&blkID, 5))
				}
			}
			blkIt.Close()
			require.NoError(t, txn.Commit(p.Ctx))
		}

		// delete two 2 rows one time. no special reason, it just comes up
		for i := 0; i < len(deleteRowIDs); i += 2 {
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			tbl, _ := db.GetRelationByName("test")
			require.NoError(t, tbl.DeleteByPhyAddrKey(deleteRowIDs[i]))
			if i+1 < len(deleteRowIDs) {
				tbl.DeleteByPhyAddrKey(deleteRowIDs[i+1])
			}
			require.NoError(t, txn.Commit(p.Ctx))
			writeTs = append(writeTs, txn.GetPrepareTS())
		}
		wg.Done()
	}()

	// concurrent read to test race
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 10; i++ {
				reader := logMgr.GetReader(minTs, maxTs)
				_ = reader.IsDirtyOnTable(dbID, tableID)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	firstWriteTs, lastWriteTs := writeTs[0], writeTs[len(writeTs)-1]

	reader = logMgr.GetReader(minTs, types.TimestampToTS(catalogWriteTs))
	require.False(t, reader.IsDirtyOnTable(dbID, tableID))
	reader = logMgr.GetReader(firstWriteTs, lastWriteTs)
	require.False(t, reader.IsDirtyOnTable(dbID, tableID-1))
	reader = logMgr.GetReader(firstWriteTs, lastWriteTs)
	require.True(t, reader.IsDirtyOnTable(dbID, tableID))

	fixedColCnt := 2 // __rowid + commit_time, the columns for a delBatch
	// check Bat rows count consistency
	check_same_rows := func(bat *api.Batch, expect int) {
		for i, vec := range bat.Vecs {
			col, err := vector.ProtoVectorToVector(vec)
			require.NoError(t, err)
			require.Equal(t, expect, col.Length(), "columns %d", i)
		}
	}

	// get db catalog change
	resp, close, err := logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(minTs),
		CnWant: &catalogDropTs,
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_DATABASE_ID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(resp.Commands)) // data_mata, tombstone_meta, insert, delete

	require.Equal(t, api.Entry_Insert, resp.Commands[2].EntryType)
	require.Equal(t, len(catalog2.SystemDBSchema.ColDefs)+1 /*commit ts*/, len(resp.Commands[2].Bat.Vecs))
	check_same_rows(resp.Commands[2].Bat, 2)                                 // 2 db
	datname, err := vector.ProtoVectorToVector(resp.Commands[2].Bat.Vecs[3]) // datname column
	require.NoError(t, err)
	require.Equal(t, "todrop", datname.UnsafeGetStringAt(0))
	require.Equal(t, "db", datname.UnsafeGetStringAt(1))

	require.Equal(t, api.Entry_Delete, resp.Commands[3].EntryType)
	require.Equal(t, fixedColCnt+2, len(resp.Commands[3].Bat.Vecs))
	check_same_rows(resp.Commands[3].Bat, 1) // 1 drop db

	close()

	// get table catalog change
	resp, close, err = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(minTs),
		CnWant: &catalogDropTs,
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(resp.Commands)) // data_mata, tombstone_meta, insert, delete
	require.Equal(t, api.Entry_Insert, resp.Commands[2].EntryType)
	require.Equal(t, len(catalog2.SystemTableSchema.ColDefs)+1, len(resp.Commands[2].Bat.Vecs))
	check_same_rows(resp.Commands[2].Bat, 2)                                 // 2 tables
	relname, err := vector.ProtoVectorToVector(resp.Commands[2].Bat.Vecs[3]) // relname column
	require.NoError(t, err)
	require.Equal(t, schema.Name, relname.UnsafeGetStringAt(0))
	require.Equal(t, schema.Name, relname.UnsafeGetStringAt(1))

	require.Equal(t, api.Entry_Delete, resp.Commands[3].EntryType)
	require.Equal(t, fixedColCnt+2, len(resp.Commands[3].Bat.Vecs)) // 4 columns, rowid + commit_ts + delete_rowid + pk
	check_same_rows(resp.Commands[3].Bat, 1)                        // 1 drop table
	close()

	// get columns catalog change
	resp, close, err = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(minTs),
		CnWant: &catalogDropTs,
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(resp.Commands)) // data_mata, tombstone_meta, insert, delete
	require.Equal(t, api.Entry_Insert, resp.Commands[2].EntryType)
	require.Equal(t, len(catalog2.SystemColumnSchema.ColDefs)+1, len(resp.Commands[2].Bat.Vecs))
	check_same_rows(resp.Commands[2].Bat, len(schema.ColDefs)*2) // column count of 2 tables
	close()

	// get user table change
	resp, close, err = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(firstWriteTs.Next()), // skip the first write deliberately,
		CnWant: totsp(lastWriteTs),
		Table:  &api.TableID{DbId: dbID, TbId: tableID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(resp.Commands)) // data_mata, tombstone_meta, insert, delete

	// check data change
	insDataEntry := resp.Commands[2]
	require.Equal(t, api.Entry_Insert, insDataEntry.EntryType)
	require.Equal(t, len(schema.ColDefs)+1, len(insDataEntry.Bat.Vecs)) // 5 columns, rowid + commit ts + 2 visibile
	check_same_rows(insDataEntry.Bat, 99)                               // 99 rows, because the first write is excluded.
	// test first user col, this is probably fragile, it depends on the details of MockSchema
	// if something changes, delete this is okay.
	firstCol, err := vector.ProtoVectorToVector(insDataEntry.Bat.Vecs[2]) // mock_0 column, int8 type
	require.Equal(t, types.T_int8, firstCol.GetType().Oid)
	require.NoError(t, err)

	delDataEntry := resp.Commands[3]
	require.Equal(t, api.Entry_Delete, delDataEntry.EntryType)
	require.Equal(t, fixedColCnt+2, len(delDataEntry.Bat.Vecs)) // 4 columns, rowid + commit_ts + delete_rowid + pk
	check_same_rows(delDataEntry.Bat, 10)

	// check delete rowids are exactly what we want
	rowids, err := vector.ProtoVectorToVector(delDataEntry.Bat.Vecs[0])
	require.NoError(t, err)
	require.Equal(t, types.T_Rowid, rowids.GetType().Oid)
	rowidMap := make(map[types.Rowid]int)
	for _, id := range deleteRowIDs {
		rowidMap[id] = 1
	}
	for i := int64(0); i < 10; i++ {
		id := vector.MustFixedColWithTypeCheck[types.Rowid](rowids)[i]
		rowidMap[id] = rowidMap[id] + 1
	}
	require.Equal(t, 10, len(rowidMap))
	for _, v := range rowidMap {
		require.Equal(t, 2, v)
	}
	close()
}

func TestAlterTableBasic(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(2, -1)
	schema.Name = "test"
	schema.Constraint = []byte("")
	initComment := "comment version; rows:10; blks:2"
	schema.Comment = initComment

	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	db, _ := p.D.Engine.Database(p.Ctx, "db", txnop)
	tbl, _ := db.Relation(p.Ctx, "test", nil)
	cstr1 := &engine.ConstraintDef{
		Cts: []engine.Constraint{&engine.RefChildTableDef{Tables: []uint64{1}}},
	}
	err := tbl.UpdateConstraint(p.Ctx, cstr1)
	require.NoError(t, err)
	err = tbl.AlterTable(p.Ctx, nil, []*api.AlterTableReq{
		api.NewUpdateCommentReq(tbl.GetDBID(p.Ctx), tbl.GetTableID(p.Ctx), "comment version 1")})
	require.NoError(t, err)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	db, _ = p.D.Engine.Database(p.Ctx, "db", txnop)
	tbl, _ = db.Relation(p.Ctx, "test", nil)
	cstr2 := &engine.ConstraintDef{
		Cts: []engine.Constraint{&engine.RefChildTableDef{Tables: []uint64{1, 2}}},
	}
	err = tbl.UpdateConstraint(p.Ctx, cstr2)
	require.NoError(t, err)
	require.NoError(t, txnop.Commit(p.Ctx))

	resp, close, _ := logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.BuildTS(0, 0)),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
	}, true)

	bat, _ := batch.ProtoBatchToBatch(resp.Commands[2].Bat)
	cstrCol := containers.NewNonNullBatchWithSharedMemory(bat, common.DefaultAllocator).GetVectorByName(catalog.SystemRelAttr_Constraint)
	require.Equal(t, 3, cstrCol.Length())
	c1, err := cstr1.MarshalBinary()
	require.NoError(t, err)
	c2, err := cstr2.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, []byte{}, cstrCol.Get(0).([]byte))
	require.Equal(t, c1, cstrCol.Get(1).([]byte))
	require.Equal(t, c2, cstrCol.Get(2).([]byte))

	commetCol := containers.NewNonNullBatchWithSharedMemory(bat, common.DefaultAllocator).GetVectorByName(catalog.SystemRelAttr_Comment)
	require.Equal(t, 3, cstrCol.Length())
	require.Equal(t, []byte(initComment), commetCol.Get(0).([]byte))
	require.Equal(t, []byte("comment version 1"), commetCol.Get(1).([]byte))
	require.Equal(t, []byte("comment version 1"), commetCol.Get(2).([]byte))

	require.Equal(t, api.Entry_Delete, resp.Commands[3].EntryType)

	close()

	txnop = p.StartCNTxn()
	db, _ = p.D.Engine.Database(p.Ctx, "db", txnop)
	require.NoError(t, db.Delete(p.Ctx, "test"))
	require.NoError(t, txnop.Commit(p.Ctx))

	resp, close, _ = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.BuildTS(0, 0)),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
	}, true)

	require.Equal(t, 4, len(resp.Commands)) // create and drop
	require.Equal(t, api.Entry_Insert, resp.Commands[2].EntryType)
	require.Equal(t, api.Entry_Delete, resp.Commands[3].EntryType)
	close()
}

func TestColumnsTransfer(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	dir := testutil.MakeDefaultTestPath("partition_state", t)
	opts.Fs = objectio.TmpNewSharedFileservice(context.Background(), dir)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(8000, -1)
	schema.Name = "test"

	schema2 := catalog2.MockSchemaAll(200, -1)
	schema2.Name = "todrop"

	txnop := p.StartCNTxn()
	p.CreateDBAndTables(txnop, "db", schema, schema2)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	txnop.GetWorkspace().StartStatement()
	p.DeleteTableInDB(txnop, "db", schema2.Name)

	txn, _ := tae.StartTxn(nil)
	catalogDB, _ := txn.GetDatabaseByID(catalog.MO_CATALOG_ID)
	columnsTbl, _ := catalogDB.GetRelationByID(catalog.MO_COLUMNS_ID)

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	it := columnsTbl.MakeObjectIt(false)
	it.Next()
	firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	t.Log(firstEntry.ID().ShortStringEx())
	task1, err := jobs.NewFlushTableTailTask(
		tasks.WaitableCtx, txn,
		[]*catalog2.ObjectEntry{firstEntry},
		nil,
		tae.Runtime)
	require.NoError(t, err)
	worker.SendOp(task1)
	err = task1.WaitDone(context.Background())
	require.NoError(t, err)

	require.NoError(t, txn.Commit(p.Ctx))

	time.Sleep(200 * time.Millisecond)
	ctx := context.WithValue(p.Ctx, disttae.UT_ForceTransCheck{}, 42)
	require.NoError(t, txnop.GetWorkspace().IncrStatementID(ctx, true))
	require.NoError(t, txnop.Commit(p.Ctx))

}

func TestInProgressTransfer(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	dir := testutil.MakeDefaultTestPath("partition_state", t)
	opts.Fs = objectio.TmpNewSharedFileservice(context.Background(), dir)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()
	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	fault.Enable()
	defer fault.Disable()
	err1 := fault.AddFaultPoint(
		p.Ctx,
		objectio.FJ_CommitDelete,
		":::",
		"echo",
		0,
		"trace delete",
		false,
	)
	require.NoError(t, err1)
	defer fault.RemoveFaultPoint(p.Ctx, objectio.FJ_CommitDelete)
	err1 = fault.AddFaultPoint(
		p.Ctx,
		objectio.FJ_CommitSlowLog,
		":::",
		"echo",
		0,
		"trace slowlog",
		false,
	)
	require.NoError(t, err1)
	defer fault.RemoveFaultPoint(p.Ctx, objectio.FJ_CommitSlowLog)

	var did, tid uint64
	var theRow *batch.Batch
	{
		var (
			dbName = "db"
			tName  = "test"
		)
		schema := catalog2.MockSchemaAll(10, 3)
		schema.Name = tName
		// create and append data
		txnop := p.StartCNTxn()
		_, rel := p.CreateDBAndTable(txnop, dbName, schema)
		did, tid = rel.GetDBID(p.Ctx), rel.GetTableID(p.Ctx)
		bat := catalog2.MockBatch(schema, 10)
		theRow = containers.ToCNBatch(bat.CloneWindow(7, 1, p.Mp))
		require.NoError(t, rel.Write(p.Ctx, containers.ToCNBatch(bat)))
		require.NoError(t, txnop.Commit(p.Ctx))
		require.Nil(t, p.D.SubscribeTable(p.Ctx, did, tid, dbName, tName, false))
	}

	toTransferTxn1 := p.StartCNTxn()
	toTransferTxn2 := p.StartCNTxn()
	{
		tnTxn, _ := tae.StartTxn(nil)
		userDB, _ := tnTxn.GetDatabaseByID(did)
		userTbl, _ := userDB.GetRelationByID(tid)
		id, row, err := userTbl.GetByFilter(p.Ctx, handle.NewEQFilter(int64(7)))
		require.NoError(t, err)
		rowid := objectio.NewRowid(&id.BlockID, row)

		vec1 := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(vec1, rowid, false, p.Mp))
		vec2 := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec2, int64(7), false, p.Mp))

		delBatch := batch.NewWithSize(2)
		delBatch.SetRowCount(1)
		delBatch.Attrs = []string{catalog.Row_ID, catalog.TableTailAttrPKVal}
		delBatch.Vecs[0] = vec1
		delBatch.Vecs[1] = vec2

		{
			db, err := p.D.Engine.Database(p.Ctx, "db", toTransferTxn1)
			require.NoError(t, err)
			rel, err := db.Relation(p.Ctx, "test", nil)
			require.NoError(t, err)
			require.NoError(t, rel.Delete(p.Ctx, delBatch, catalog.Row_ID))
			require.NoError(t, rel.Write(p.Ctx, theRow))
		}

		{
			db, err := p.D.Engine.Database(p.Ctx, "db", toTransferTxn2)
			require.NoError(t, err)
			rel, err := db.Relation(p.Ctx, "test", nil)
			require.NoError(t, err)
			require.NoError(t, rel.Delete(p.Ctx, delBatch, catalog.Row_ID))
		}
	}

	{
		// first flush, with a parallel updating
		tnFlushTxn, _ := tae.StartTxn(nil)
		userDB, _ := tnFlushTxn.GetDatabaseByID(did)
		userTbl, _ := userDB.GetRelationByID(tid)

		it := userTbl.MakeObjectIt(false)
		it.Next()
		firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
		firstEntry.GetObjectData().FreezeAppend()
		t.Log(firstEntry.ID().ShortStringEx())
		task1, err := jobs.NewFlushTableTailTask(
			tasks.WaitableCtx, tnFlushTxn,
			[]*catalog2.ObjectEntry{firstEntry},
			nil,
			tae.Runtime)
		require.NoError(t, err)
		worker.SendOp(task1)
		err = task1.WaitDone(context.Background())
		require.NoError(t, err)

		{
			// update during flushing, create a new aobject
			tnDelTxn, _ := tae.StartTxn(nil)
			userDB, _ := tnDelTxn.GetDatabaseByID(did)
			userTbl, _ := userDB.GetRelationByID(tid)
			require.NoError(t, userTbl.UpdateByFilter(p.Ctx, handle.NewEQFilter(int64(7)), 0, int8(42), false))
			require.NoError(t, tnDelTxn.Commit(p.Ctx))
		}

		require.NoError(t, tnFlushTxn.Commit(p.Ctx))
	}

	{
		// trigger first transfer, read the memory delete and also find the the pk in memroy
		time.Sleep(200 * time.Millisecond)
		ctx := context.WithValue(p.Ctx, disttae.UT_ForceTransCheck{}, 42)
		require.NoError(t, toTransferTxn1.GetWorkspace().IncrStatementID(ctx, true))
		require.NoError(t, toTransferTxn1.Commit(p.Ctx))
	}

	{
		// flush the second abobject
		tnFlushTxn2, _ := tae.StartTxn(nil)
		userDB, _ := tnFlushTxn2.GetDatabaseByID(did)
		userTbl, _ := userDB.GetRelationByID(tid)
		it := userTbl.MakeObjectIt(false)
		var entry *catalog2.ObjectEntry
		for it.Next() {
			entry = it.GetObject().GetMeta().(*catalog2.ObjectEntry)
			if entry.IsAppendable() && !entry.HasDropCommitted() {
				break
			}
		}
		task1, err := jobs.NewFlushTableTailTask(
			tasks.WaitableCtx, tnFlushTxn2,
			[]*catalog2.ObjectEntry{entry},
			nil,
			tae.Runtime)
		require.NoError(t, err)
		worker.SendOp(task1)
		err = task1.WaitDone(context.Background())
		require.NoError(t, err)
		require.NoError(t, tnFlushTxn2.Commit(p.Ctx))
	}

	{
		// trigge the second transfer, find the pk in the first row of the latest nobject
		time.Sleep(200 * time.Millisecond)
		ctx := context.WithValue(p.Ctx, disttae.UT_ForceTransCheck{}, 42)
		require.NoError(t, toTransferTxn2.GetWorkspace().IncrStatementID(ctx, true))
		require.NoError(t, toTransferTxn2.Commit(p.Ctx))
	}
}

func TestCacheGC(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()

	schema1 := catalog2.MockSchemaAll(13, 3)
	schema1.Name = "test1"
	schema2 := catalog2.MockSchemaAll(13, 3)
	schema2.Name = "test2"
	schema3 := catalog2.MockSchemaAll(13, 3)
	schema3.Name = "test3"
	schema4 := catalog2.MockSchemaAll(13, 3)
	schema4.Name = "test4"

	txnop := p.StartCNTxn()
	p.CreateDBAndTables(txnop, "db", schema1, schema2, schema3)
	require.NoError(t, txnop.Commit(p.Ctx))

	// test1: I D I | gc 2
	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema1.Name)
	require.NoError(t, txnop.Commit(p.Ctx))
	txnop = p.StartCNTxn()
	p.CreateTableInDB(txnop, "db", schema1)
	require.NoError(t, txnop.Commit(p.Ctx))

	// test2: I D   | gc 2
	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema2.Name)
	require.NoError(t, txnop.Commit(p.Ctx))

	// test3 I      | gc 0

	gcTime := txnop.Txn().CommitTS.Next()

	// test4        | I D I gc 0
	txnop = p.StartCNTxn()
	p.CreateTableInDB(txnop, "db", schema4)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema4.Name)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn() // wait the last txn to be committed
	require.NoError(t, txnop.Commit(p.Ctx))

	// gc
	cc := p.D.Engine.GetLatestCatalogCache()
	r := cc.GC(gcTime)
	require.Equal(t, 4, r.TStaleItem)
	require.Equal(t, 2 /*test2 & test 4*/, r.TStaleCpk)

}

func TestShowDatabasesInRestoreTxn(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(10, -1)
	schema.Name = "test"
	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	ts := time.Now().UTC().UnixNano()

	schema2 := catalog2.MockSchemaAll(10, -1)
	schema2.Name = "test2"
	txnop = p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db2", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	txn, _ := tae.StartTxn(nil)
	catalogDB, _ := txn.GetDatabaseByID(catalog.MO_CATALOG_ID)
	dbTbl, _ := catalogDB.GetRelationByID(catalog.MO_DATABASE_ID)

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	it := dbTbl.MakeObjectIt(false)
	it.Next()
	firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	task1, err := jobs.NewFlushTableTailTask(
		tasks.WaitableCtx, txn,
		[]*catalog2.ObjectEntry{firstEntry},
		nil,
		tae.Runtime)
	require.NoError(t, err)
	worker.SendOp(task1)
	err = task1.WaitDone(context.Background())
	require.NoError(t, err)

	require.NoError(t, txn.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	require.NoError(t, p.D.Engine.Delete(p.Ctx, "db2", txnop))
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", ""))
	}
	exec := v.(executor.SQLExecutor)
	res, err := exec.Exec(p.Ctx, fmt.Sprintf("show databases {MO_TS=%d}", ts), executor.Options{}.WithTxn(txnop))
	require.NoError(t, err)
	var rels []string
	for _, b := range res.Batches {
		for i, v := 0, b.Vecs[0]; i < v.Length(); i++ {
			rels = append(rels, v.GetStringAt(i))
		}
	}
	require.Equal(t, 2, len(rels), rels) // mo_catalog + db
	require.NotContains(t, rels, "db2")
}

func TestObjectStats1(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	schema := catalog2.MockSchemaAll(10, 0)
	bat := catalog2.MockBatch(schema, 10)

	testutil2.CreateRelationAndAppend(t, catalog.System_Account, taeHandler.GetDB(), "db", schema, bat, true)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txn, rel := testutil2.GetRelation(t, catalog.System_Account, taeHandler.GetDB(), "db", schema.Name)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	appendableObjectID := testutil2.GetOneObject(rel).GetID()
	id.SetObjectID(appendableObjectID)
	err := rel.RangeDelete(id, 0, 0, handle.DT_Normal)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, 0, taeHandler.GetDB(), "db", schema, false)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, "db", schema.Name, false)
	require.Nil(t, err)

	ts := taeHandler.GetDB().TxnMgr.Now()
	state := disttaeEngine.Engine.GetOrCreateLatestPart(id.DbID, id.TableID).Snapshot()
	iter, err := state.NewObjectsIter(ts, false, false)
	assert.NoError(t, err)
	objCount := 0
	for iter.Next() {
		obj := iter.Entry()
		objCount++
		if bytes.Equal(obj.ObjectStats.ObjectName().ObjectId()[:], appendableObjectID[:]) {
			assert.True(t, obj.GetAppendable())
			assert.False(t, obj.GetSorted())
			assert.False(t, obj.ObjectStats.GetCNCreated())
		} else {
			assert.False(t, obj.GetAppendable())
			assert.True(t, obj.GetSorted())
			assert.False(t, obj.ObjectStats.GetCNCreated())
		}
	}
	assert.Equal(t, objCount, 2)
	iter.Close()
	iter, err = state.NewObjectsIter(ts, false, true)
	assert.NoError(t, err)
	objCount = 0
	appendableCount := 0
	for iter.Next() {
		obj := iter.Entry()
		objCount++
		if obj.GetAppendable() {
			appendableCount++
		}
		assert.True(t, obj.GetSorted())
		assert.False(t, obj.ObjectStats.GetCNCreated())
	}
	assert.Equal(t, appendableCount, 1)
	assert.Equal(t, objCount, 2)
	iter.Close()

	testutil2.MergeBlocks(t, 0, taeHandler.GetDB(), "db", schema, false)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	ts = taeHandler.GetDB().TxnMgr.Now()
	state = disttaeEngine.Engine.GetOrCreateLatestPart(id.DbID, id.TableID).Snapshot()
	iter, err = state.NewObjectsIter(ts, true, false)
	assert.NoError(t, err)
	objCount = 0
	for iter.Next() {
		obj := iter.Entry()
		objCount++
		assert.False(t, obj.GetAppendable())
		assert.True(t, obj.GetSorted())
		assert.False(t, obj.ObjectStats.GetCNCreated())
	}
	assert.Equal(t, objCount, 1)
	iter.Close()
}

func TestObjectStats2(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	schema := catalog2.MockSchemaAll(10, -1)
	bat := catalog2.MockBatch(schema, 10)

	testutil2.CreateRelationAndAppend(t, catalog.System_Account, taeHandler.GetDB(), "db", schema, bat, true)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txn, rel := testutil2.GetRelation(t, catalog.System_Account, taeHandler.GetDB(), "db", schema.Name)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	appendableObjectID := testutil2.GetOneObject(rel).GetID()
	id.SetObjectID(appendableObjectID)
	err := rel.RangeDelete(id, 0, 0, handle.DT_Normal)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, 0, taeHandler.GetDB(), "db", schema, false)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, "db", schema.Name, false)
	require.Nil(t, err)

	ts := taeHandler.GetDB().TxnMgr.Now()
	state := disttaeEngine.Engine.GetOrCreateLatestPart(id.DbID, id.TableID).Snapshot()
	iter, err := state.NewObjectsIter(ts, false, false)
	assert.NoError(t, err)
	objCount := 0
	for iter.Next() {
		obj := iter.Entry()
		objCount++
		if bytes.Equal(obj.ObjectStats.ObjectName().ObjectId()[:], appendableObjectID[:]) {
			assert.True(t, obj.GetAppendable())
			assert.False(t, obj.GetSorted())
			assert.False(t, obj.ObjectStats.GetCNCreated())
		} else {
			assert.False(t, obj.GetAppendable())
			assert.False(t, obj.GetSorted())
			assert.False(t, obj.ObjectStats.GetCNCreated())
		}
	}
	assert.Equal(t, objCount, 2)
	iter.Close()
	iter, err = state.NewObjectsIter(ts, false, true)
	assert.NoError(t, err)
	objCount = 0
	appendableCount := 0
	for iter.Next() {
		obj := iter.Entry()
		objCount++
		if obj.GetAppendable() {
			appendableCount++
		}
		assert.True(t, obj.GetSorted())
		assert.False(t, obj.ObjectStats.GetCNCreated())
	}
	assert.Equal(t, appendableCount, 1)
	assert.Equal(t, objCount, 2)
	iter.Close()

	testutil2.MergeBlocks(t, 0, taeHandler.GetDB(), "db", schema, false)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	ts = taeHandler.GetDB().TxnMgr.Now()
	state = disttaeEngine.Engine.GetOrCreateLatestPart(id.DbID, id.TableID).Snapshot()
	iter, err = state.NewObjectsIter(ts, true, false)
	assert.NoError(t, err)
	objCount = 0
	for iter.Next() {
		obj := iter.Entry()
		objCount++
		assert.False(t, obj.GetAppendable())
		assert.False(t, obj.GetSorted())
		assert.False(t, obj.ObjectStats.GetCNCreated())
	}
	iter.Close()
	assert.Equal(t, objCount, 1)
}

func TestApplyDeletesForWorkspaceAndPart(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()
	fault.Enable()
	defer fault.Disable()
	rmFault, err := objectio.InjectLog1(
		"mo_account",
		2,
	)
	require.NoError(t, err)
	defer rmFault()
	var (
		dbName = "db"
		tName  = "mo_account"
	)
	schema := catalog2.MockSchemaAll(5, 1)
	schema.Name = tName
	txnop := p.StartCNTxn()
	bats := catalog2.MockBatch(schema, 4).Split(2)
	bat, bat1 := bats[0], bats[1]
	_, rel := p.CreateDBAndTable(txnop, dbName, schema)
	rel.Write(p.Ctx, containers.ToCNBatch(bat)) // pk 0 and 1
	require.NoError(t, txnop.Commit(p.Ctx))

	did, tid := rel.GetDBID(p.Ctx), rel.GetTableID(p.Ctx)
	require.NoError(t, p.D.SubscribeTable(p.Ctx, did, tid, dbName, tName, false))

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", ""))
	}
	exec := v.(executor.SQLExecutor)

	txnop = p.StartCNTxn()
	_, err = exec.Exec(p.Ctx, "delete from db.mo_account where mock_1 in (0, 1)", executor.Options{}.WithTxn(txnop))
	require.NoError(t, err)
	require.NoError(t, txnop.Commit(p.Ctx))

	txn, _ := tae.StartTxn(nil)
	udb, _ := txn.GetDatabaseByID(did)
	utbl, _ := udb.GetRelationByID(tid)

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	// flusht tombstone ahead of data insert
	it := utbl.MakeObjectIt(true)
	it.Next()
	firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	t.Log(firstEntry.ID().ShortStringEx())
	task1, err := jobs.NewFlushTableTailTask(
		tasks.WaitableCtx, txn,
		nil,
		[]*catalog2.ObjectEntry{firstEntry},
		tae.Runtime)
	require.NoError(t, err)
	worker.SendOp(task1)
	err = task1.WaitDone(context.Background())
	require.NoError(t, err)
	require.NoError(t, txn.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	db, err := p.D.Engine.Database(p.Ctx, "db", txnop)
	require.NoError(t, err)
	rel, err = db.Relation(p.Ctx, "mo_account", nil)
	require.NoError(t, err)
	require.NoError(t, rel.Write(p.Ctx, containers.ToCNBatch(bat1))) // pk 2 and 3
	res, err := exec.Exec(p.Ctx, "select * from db.mo_account", executor.Options{}.WithTxn(txnop))
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Batches))
	require.Equal(t, 2, res.Batches[0].RowCount())
	pkSum := int16(0)
	for i := 0; i < 2; i++ {
		pkSum += vector.GetFixedAtWithTypeCheck[int16](res.Batches[0].Vecs[1], i)
	}
	require.Equal(t, int16(5 /*2+3*/), pkSum)
}

func TestApplyDeletesFromTombstoneObjects(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(8189, 1)
	schema.Name = "padding"
	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db", schema)

	schema2 := catalog2.MockSchemaAll(4, 1)
	schema2.Name = "table2"
	p.CreateTableInDB(txnop, "db", schema2)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema.Name)
	p.DeleteTableInDB(txnop, "db", schema2.Name)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	rel := p.CreateTableInDB(txnop, "db", schema2)
	require.NoError(t, txnop.Commit(p.Ctx))

	querySql := fmt.Sprintf(catalog.MoColumnsRowidsQueryFormat, 0, "db", "table2", rel.GetTableID(p.Ctx))

	txn, _ := tae.StartTxn(nil)
	udb, _ := txn.GetDatabaseByID(catalog.MO_CATALOG_ID)
	utbl, _ := udb.GetRelationByID(catalog.MO_COLUMNS_ID)
	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()
	// flusht tombstone ahead of data insert
	it := utbl.MakeObjectIt(true)
	require.True(t, it.Next())
	firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	require.True(t, it.Next())
	secondEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	t.Log(firstEntry.ID().ShortStringEx())
	task1, err := jobs.NewFlushTableTailTask(
		tasks.WaitableCtx, txn,
		nil,
		[]*catalog2.ObjectEntry{firstEntry, secondEntry},
		tae.Runtime)
	require.NoError(t, err)
	worker.SendOp(task1)
	err = task1.WaitDone(context.Background())
	require.NoError(t, err)
	require.NoError(t, txn.Commit(p.Ctx))

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", ""))
	}
	exec := v.(executor.SQLExecutor)
	txnop = p.StartCNTxn()
	buf := new(bytes.Buffer)
	ctx := context.WithValue(p.Ctx, defines.ReaderSummaryKey{}, buf)
	result, err := exec.Exec(ctx, querySql, executor.Options{}.WithTxn(txnop))
	require.NoError(t, err)
	t.Log("readerSummary", buf.String())
	require.Equal(t, 1, len(result.Batches))
	require.Equal(t, 5, result.Batches[0].RowCount())
	require.NoError(t, txnop.Commit(p.Ctx))
}

func TestCache3Tables(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	txnop := p.StartCNTxn()
	dbname, tname, rel, err := p.D.Engine.GetRelationById(p.Ctx, txnop, catalog.MO_DATABASE_ID)
	require.NoError(t, err)
	require.Equal(t, catalog.MO_CATALOG, dbname)
	require.Equal(t, catalog.MO_DATABASE, tname)
	require.NotNil(t, rel)

	dbname, tname, rel, err = p.D.Engine.GetRelationById(p.Ctx, txnop, catalog.MO_TABLES_ID)
	require.NoError(t, err)
	require.Equal(t, catalog.MO_CATALOG, dbname)
	require.Equal(t, catalog.MO_TABLES, tname)
	require.NotNil(t, rel)

	dbname, tname, rel, err = p.D.Engine.GetRelationById(p.Ctx, txnop, catalog.MO_COLUMNS_ID)
	require.NoError(t, err)
	require.Equal(t, catalog.MO_CATALOG, dbname)
	require.Equal(t, catalog.MO_COLUMNS, tname)
	require.NotNil(t, rel)
}

func TestRelationExists(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	txnop := p.StartCNTxn()
	schema := catalog2.MockSchemaAll(2, 0)
	schema.Name = "test"
	p.CreateDBAndTable(txnop, "db", schema)

	db, err := p.D.Engine.Database(p.Ctx, "db", txnop)
	require.NoError(t, err)

	exist, err := db.RelationExists(p.Ctx, "test", nil)
	require.NoError(t, err)
	require.True(t, exist)

	exist, err = db.RelationExists(p.Ctx, "testxx", nil)
	require.NoError(t, err)
	require.False(t, exist)

	// craft no-account-id error
	ctx := context.WithValue(p.Ctx, defines.TenantIDKey{}, nil)
	exist, err = db.RelationExists(ctx, "testxx", nil)
	require.Error(t, err)
	require.False(t, exist)

	rel, err := db.Relation(ctx, "test", nil)
	require.Error(t, err)
	require.Nil(t, rel)
}

func TestWorkspaceQuota(t *testing.T) {
	quotaSize := uint64(1000)

	p := testutil.InitEnginePack(testutil.TestOptions{
		DisttaeOptions: []testutil.TestDisttaeEngineOptions{testutil.WithDisttaeEngineQuota(quotaSize)}}, t)
	defer p.Close()
	e := p.D.Engine
	var wg sync.WaitGroup
	wg.Add(10)
	for i := range uint64(10) {
		go func(size uint64, wg *sync.WaitGroup) {
			e.AcquireQuota(100)
			e.ReleaseQuota(100)
			wg.Done()
		}(i*100, &wg)
	}

	wg.Wait()
	remaining, _ := e.AcquireQuota(0)
	require.Equal(t, int(quotaSize), int(remaining))
	_, acquired := e.AcquireQuota(quotaSize + 1)
	require.False(t, acquired)
}

func TestWorkspaceQuota2(t *testing.T) {

	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(1),
		testutil.WithDisttaeEngineQuota(800),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 30
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1, _ := containers.ToCNBatch(bat).Window(0, 10)
	bat2, _ := containers.ToCNBatch(bat).Window(10, 20)
	bat3, _ := containers.ToCNBatch(bat).Window(20, 30)

	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		// exceed workspace write threshold, acquire quota success, do not write s3
		require.NoError(t, relation.Write(ctx, bat1))
		// exceed workspace write threshold, acquire quota success, do not write s3
		require.NoError(t, relation.Write(ctx, bat2))
		// exceed workspace write threshold, acquire quota failed,  write s3
		require.NoError(t, relation.Write(ctx, bat3))
		// exceed workspace commit threshold, write s3
		require.NoError(t, txn.Commit(ctx))
	}

	require.NoError(t, disttaeEngine.SubscribeTable(ctx, relation.GetDBID(ctx), relation.GetTableID(ctx), databaseName, tableName, false))
	state, err := disttaeEngine.GetPartitionStateStats(ctx, relation.GetDBID(ctx), relation.GetTableID(ctx))
	require.NoError(t, err)
	t.Log(state.String())
	require.Equal(t, 1, state.DataObjectsVisible.ObjCnt)

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)

	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	cnt := 0
	for {
		ok, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()
		if ok {
			break
		}
	}
	require.NoError(t, err)

	require.Equal(t, rowsCount, cnt)
	require.NoError(t, txn.Commit(ctx))
}

func TestWorkspaceQuota3(t *testing.T) {

	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(1),
		testutil.WithDisttaeEngineQuota(10000),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 30
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1, _ := containers.ToCNBatch(bat).Window(0, 10)
	bat2, _ := containers.ToCNBatch(bat).Window(10, 20)
	bat3, _ := containers.ToCNBatch(bat).Window(20, 30)

	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, relation.Write(ctx, bat1))
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, relation.Write(ctx, bat3))
		require.NoError(t, txn.Rollback(ctx))
	}

	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, relation.Write(ctx, bat1))
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, relation.Write(ctx, bat3))
		require.NoError(t, txn.Commit(ctx))
	}

	// test quota leak
	quota, _ := disttaeEngine.Engine.AcquireQuota(0)
	require.Equal(t, 10000, int(quota))

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)

	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	cnt := 0
	for {
		ok, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()
		if ok {
			break
		}
	}
	require.NoError(t, err)

	require.Equal(t, rowsCount, cnt)
	require.NoError(t, txn.Commit(ctx))
}

func TestCacheNotServing(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	txnop := p.StartCNTxn()
	schema := catalog2.MockSchemaAll(2, 0)
	schema.Name = "test"
	p.CreateDBAndTable(txnop, "db", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	staleTxn := p.StartCNTxn()

	txnop = p.StartCNTxn()
	p.D.Engine.Delete(p.Ctx, "db", txnop)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	p.D.Engine.GetLatestCatalogCache().GC(txnop.SnapshotTS())
	require.NoError(t, txnop.Commit(p.Ctx))

	db, err := p.D.Engine.Database(p.Ctx, "db", staleTxn)
	require.NoError(t, err)

	exist, err := db.RelationExists(p.Ctx, "test", nil)
	require.NoError(t, err)
	require.True(t, exist)
	rel, err := db.Relation(p.Ctx, "test", nil)
	require.NoError(t, err)

	dname, tname, _, _ := p.D.Engine.GetRelationById(p.Ctx, staleTxn, rel.GetTableID(p.Ctx))
	require.Equal(t, "db", dname)
	require.Equal(t, "test", tname)

	_, err = rel.Ranges(p.Ctx, engine.DefaultRangesParam)
	t.Log(err)

	require.NoError(t, staleTxn.Commit(p.Ctx))
}

func TestInvalidTxnOp(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	txnop := p.StartCNTxn()

	schema := catalog2.MockSchemaAll(2, 0)
	schema.Name = "test"
	p.CreateDBAndTable(txnop, "db", schema)

	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()

	userDb, err := p.D.Engine.Database(p.Ctx, "db", txnop)
	require.NoError(t, err)

	require.NoError(t, txnop.Commit(p.Ctx))

	_, err = userDb.Relation(p.Ctx, "test", nil)
	require.Error(t, err)
}

func Test_SubUnsubTable(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	err = disttaeEngine.Engine.Create(ctx, databaseName, txn)
	require.Nil(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txn)
	require.Nil(t, err)

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName

	defs, err := testutil.EngineTableDefBySchema(schema)
	require.Nil(t, err)

	err = db.Create(ctx, tableName, defs)
	require.Nil(t, err)

	rel, err := db.Relation(ctx, tableName, nil)
	require.Nil(t, err)
	require.Contains(t, rel.GetTableName(), tableName)

	bat := catalog2.MockBatch(schema, 10)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.Nil(t, err)

	err = txn.Commit(ctx)
	require.Nil(t, err)

	//subscribe a valid table
	require.Nil(t, disttaeEngine.SubscribeTable(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx), databaseName, tableName, false))

	//subscribe a invalid table
	var (
		inValidTableID   = rel.GetTableID(ctx) + 1
		inValidTableName = "invalid_table"
	)
	require.NotNil(t, disttaeEngine.SubscribeTable(ctx, rel.GetDBID(ctx), inValidTableID, databaseName, inValidTableName, false))
}

func TestDeleteTupleInTupleList(t *testing.T) {
	p := testutil.InitEnginePack(testutil.TestOptions{}, t)
	defer p.Close()

	v, _ := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	exec := v.(executor.SQLExecutor)

	var (
		dbName = "db"
		tName  = "t"
	)
	schema := catalog2.MockSchemaAll2(20, []int{1, 2})
	schema.Name = tName
	txnop := p.StartCNTxn()
	_, _ = p.CreateDBAndTable(txnop, dbName, schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	query := `delete from db.t where (mock_1, mock_2) in ((1, 2), (3, 4))`
	_, err := exec.Exec(p.Ctx, query, executor.Options{}.WithTxn(txnop))
	require.NoError(t, err)
	require.NoError(t, txnop.Commit(p.Ctx))
}
