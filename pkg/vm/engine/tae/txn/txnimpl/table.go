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

package txnimpl

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"go.uber.org/zap"
)

const (
	TransferSinkerBufferSize          = common.Const1MBytes * 5
	TransferSinkerMemorySizeThreshold = common.Const1MBytes * 50
)

type txnEntries struct {
	entries []txnif.TxnEntry
	mask    *roaring.Bitmap
}

func newTxnEntries() *txnEntries {
	return &txnEntries{
		entries: make([]txnif.TxnEntry, 0),
		mask:    roaring.New(),
	}
}

func (entries *txnEntries) Len() int {
	return len(entries.entries)
}

func (entries *txnEntries) Append(entry txnif.TxnEntry) {
	entries.entries = append(entries.entries, entry)
}

func (entries *txnEntries) Delete(idx int) {
	entries.mask.Add(uint32(idx))
}

func (entries *txnEntries) IsDeleted(idx int) bool {
	return entries.mask.ContainsInt(idx)
}

func (entries *txnEntries) AnyDelete() bool {
	return !entries.mask.IsEmpty()
}

func (entries *txnEntries) Close() {
	entries.mask = nil
	entries.entries = nil
}

type txnTable struct {
	store *txnStore

	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	entry       *catalog.TableEntry
	logs        []wal.LogEntry
	txnEntries  *txnEntries
	csnStart    uint32

	dataTable      *baseTable
	tombstoneTable *baseTable
	transferedTS   types.TS

	dedupTS types.TS

	idx int
}

func newTxnTable(store *txnStore, entry *catalog.TableEntry) (*txnTable, error) {
	schema := entry.GetVisibleSchema(store.txn, false)
	if schema == nil {
		return nil, moerr.NewInternalErrorNoCtxf("No visible schema for ts %s", store.txn.GetStartTS().ToString())
	}
	tbl := &txnTable{
		store:      store,
		entry:      entry,
		logs:       make([]wal.LogEntry, 0),
		txnEntries: newTxnEntries(),
	}
	tbl.dataTable = newBaseTable(schema, false, tbl)
	if schema.HasPK() {
		tombstoneSchema := entry.GetVisibleSchema(store.txn, true)
		tbl.tombstoneTable = newBaseTable(tombstoneSchema, true, tbl)
	}
	return tbl, nil
}

func (tbl *txnTable) getBaseTable(isTombstone bool) *baseTable {
	if isTombstone {
		return tbl.tombstoneTable
	}
	return tbl.dataTable
}
func (tbl *txnTable) PrePreareTransfer(
	ctx context.Context, phase string, ts types.TS,
) (err error) {
	err = tbl.TransferDeletes(ctx, ts, phase)
	tbl.transferedTS = ts
	return
}

func (tbl *txnTable) TransferDeleteIntent(
	id *common.ID,
	row uint32) (changed bool, nid *common.ID, nrow uint32, err error) {
	pinned, err := tbl.store.rt.TransferTable.Pin(*id)
	if err != nil {
		err = nil
		return
	}
	defer pinned.Close()
	entry, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		true)
	if err != nil {
		panic(err)
	}
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	if err = readWriteConfilictCheck(entry, ts, false); err == nil {
		return
	}
	err = nil
	nid = &common.ID{
		TableID: id.TableID,
	}
	rowID, ok := pinned.Item().Transfer(row)
	if !ok {
		err = moerr.NewTxnWWConflictNoCtx(0, "")
		return
	}
	changed = true
	bid, nrow := rowID.Decode()
	nid.BlockID = *bid
	return
}

func (tbl *txnTable) approxSize() int {
	return tbl.dataTable.approxSize() + tbl.tombstoneTable.approxSize()
}
func (tbl *txnTable) recurTransferS3Delete(
	id *common.ID,
	rowID types.Rowid,
	memo map[types.Blockid]*common.PinnedItem[*model.TransferHashPage],
	softDeletes map[objectio.ObjectId]struct{},
	phase string,
	from, to types.TS,
) (newID types.Rowid, err error) {
	blkID2, row := rowID.Decode()
	id.BlockID = *blkID2
	pinned, ok := memo[*blkID2]
	if !ok {
		pinned, err = tbl.store.rt.TransferTable.Pin(*id)
		// cannot find a transferred record. maybe the transferred record was TTL'ed
		// here we can convert the error back to r-w conflict
		if err != nil {
			logutil.Error(
				"TRANSFER-ERR-TO-RW",
				zap.Error(err),
				zap.String("id", id.String()),
				zap.String("txn", tbl.store.txn.String()),
				zap.String("phase", phase),
				zap.String("from", from.ToString()),
				zap.String("to", to.ToString()),
			)
			err = moerr.NewTxnRWConflictNoCtx()
			return
		}
	}
	page := pinned.Item()
	newID, ok = page.Transfer(row)
	if !ok {
		err = moerr.NewTxnWWConflictNoCtx(0, "")
		msg := fmt.Sprintf("table-%d blk-%d delete row-%d",
			id.TableID,
			id.BlockID,
			row)
		logutil.Warnf("[ts=%s]TransferDeleteNode: %v",
			tbl.store.txn.GetStartTS().ToString(),
			msg)
		return
	}
	blkID, _ := newID.Decode()
	objID := blkID.Object()
	_, ok = softDeletes[*objID]
	if !ok {
		return
	}
	return tbl.recurTransferS3Delete(id, newID, memo, softDeletes, phase, from, to)
}
func (tbl *txnTable) TransferDeletes(
	ctx context.Context,
	ts types.TS,
	phase string,
) (err error) {
	if tbl.store.rt.TransferTable == nil {
		return
	}
	if tbl.tombstoneTable == nil || tbl.tombstoneTable.tableSpace == nil {
		return
	}
	id := tbl.entry.AsCommonID()
	var softDeleteObjects []*catalog.ObjectEntry
	if len(tbl.tombstoneTable.tableSpace.stats) != 0 {
		tGetSoftdeleteObjects := time.Now()
		startTS := tbl.transferedTS.Next()
		dedupType := tbl.store.txn.GetDedupType()
		if dedupType.SkipTargetOldCommitted() {
			txnStartTS := tbl.store.txn.GetStartTS()
			if txnStartTS.GT(&startTS) {
				startTS = txnStartTS
			}
		}
		softDeleteObjects = tbl.entry.GetSoftdeleteObjects(startTS, ts)
		sort.Slice(softDeleteObjects, func(i, j int) bool {
			return softDeleteObjects[i].CreatedAt.LE(&softDeleteObjects[j].CreatedAt)
		})
		v2.TxnS3TombstoneTransferGetSoftdeleteObjectsHistogram.Observe(time.Since(tGetSoftdeleteObjects).Seconds())
		v2.TxnS3TombstoneSoftdeleteObjectCounter.Add(float64(len(softDeleteObjects)))
		var findTombstoneDuration, readTombstoneDuration, deleteRowsDuration time.Duration
		var sinker *ioutil.Sinker
		defer func() {
			if sinker != nil {
				sinker.Close()
			}
		}()
		// transfer deltaloc
		memo := make(map[types.Blockid]*common.PinnedItem[*model.TransferHashPage])
		objMap := make(map[types.Objectid]struct{})
		for _, obj := range softDeleteObjects {
			objMap[*obj.ID()] = struct{}{}
		}
		for _, obj := range softDeleteObjects {
			var currentTransferBatch *containers.Batch
			tFindTombstone := time.Now()
			sel, err := ioutil.FindTombstonesOfObject(
				ctx, obj.ID(), tbl.tombstoneTable.tableSpace.stats, tbl.store.rt.Fs,
			)
			findTombstoneDuration += time.Since(tFindTombstone)
			if err != nil {
				return err
			}
			id := obj.AsCommonID()
			if sel.IsEmpty() {
				continue
			}

			v2.TxnS3TombstoneTransferDataObjectCounter.Add(1)
			v2.TxnS3TombstoneTransferStatsCounter.Add(float64(sel.Count()))
			iter := sel.Iterator()
			pkType := &tbl.GetLocalSchema(false).GetPrimaryKey().Type
			transferFn := func(pkVec, rowIDVec containers.Vector) (err error) {
				rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec.GetDownstreamVector())
				for i := 0; i < rowIDVec.Length(); i++ {
					rowID := rowids[i]
					blkID2, _ := rowID.Decode()
					if !blkID2.Object().EQ(obj.ID()) {
						continue
					}
					var newRowID types.Rowid
					newRowID, err = tbl.recurTransferS3Delete(
						id,
						rowID,
						memo,
						objMap,
						phase,
						startTS,
						ts,
					)
					if err != nil {
						return
					}
					pk := pkVec.Get(i)
					// try to transfer the delete node
					// here are some possible returns
					// nil: transferred successfully
					// ErrTxnRWConflict: the target block was also be compacted
					// ErrTxnWWConflict: w-w error
					tDeleteRows := time.Now()
					if currentTransferBatch == nil {
						currentTransferBatch = catalog.NewCNTombstoneBatchByPKType(*pkType, common.WorkspaceAllocator)
					}
					currentTransferBatch.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Append(pk, false)
					currentTransferBatch.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).Append(newRowID, false)
					deleteRowsDuration += time.Since(tDeleteRows)
				}
				return
			}
			for iter.HasNext() {
				statsOffset := iter.Next()
				stats := tbl.tombstoneTable.tableSpace.stats[statsOffset]
				for i := 0; i < int(stats.BlkCnt()); i++ {
					tReadTombstone := time.Now()
					loc := stats.BlockLocation(uint16(i), tbl.tombstoneTable.schema.Extra.BlockMaxRows)
					vectors, closeFunc, err := ioutil.LoadColumns2(
						tbl.store.ctx,
						[]uint16{0, 1},
						nil,
						tbl.store.rt.Fs,
						loc,
						fileservice.Policy(0),
						false,
						nil,
					)
					readTombstoneDuration += time.Since(tReadTombstone)
					if err != nil {
						closeFunc()
						return err
					}
					err = transferFn(vectors[1], vectors[0])
					closeFunc()
					if err != nil {
						return err
					}
				}
			}
			tbl.store.warChecker.Delete(id)
			if currentTransferBatch != nil {
				if sinker == nil {
					sinker = ioutil.NewTombstoneSinker(
						objectio.HiddenColumnSelection_None,
						*pkType,
						common.WorkspaceAllocator,
						tbl.store.rt.Fs,
						ioutil.WithBufferSizeCap(TransferSinkerBufferSize),
						ioutil.WithMemorySizeThreshold(TransferSinkerMemorySizeThreshold))
				}
				sinker.Write(ctx, containers.ToCNBatch(currentTransferBatch))
				currentTransferBatch.Close()
			}
		}
		if sinker != nil {
			sinker.Sync(ctx)
			stats, bats := sinker.GetResult()

			tbl.tombstoneTable.tableSpace.registerStats(stats...)

			if len(bats) != 0 {
				panic(fmt.Sprintf("TN-TRANSFER-TOMBSTONE-FILES, batch is %d", len(bats)))
			}
			logutil.Info(
				"TN-TRANSFER-TOMBSTONE-FILES",
				zap.String("table", tbl.GetLocalSchema(false).Name),
				zap.String("phase", phase),
				zap.String("from", startTS.ToString()),
				zap.String("to", ts.ToString()),
				zap.Int("s-cnt", len(softDeleteObjects)),
				zap.String("txn", tbl.store.txn.String()),
			)
		}
		v2.TxnS3TombstoneTransferFindTombstonesHistogram.Observe(findTombstoneDuration.Seconds())
		v2.TxnS3TombstoneTransferReadTombstoneHistogram.Observe(readTombstoneDuration.Seconds())
		v2.TxnS3TombstoneTransferDeleteRowsHistogram.Observe(deleteRowsDuration.Seconds())
	}
	transferd := nulls.Nulls{}
	// transfer in memory deletes
	if tbl.tombstoneTable.tableSpace.node == nil {
		return
	}
	deletes := tbl.tombstoneTable.tableSpace.node.data
	pkVec := deletes.GetVectorByName(objectio.TombstoneAttr_PK_Attr)
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](
		deletes.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector(),
	)
	var pkType *types.Type
	for i, end := 0, len(rowids); i < end; i++ {
		rowID := &rowids[i]
		id.SetObjectID(rowID.BorrowObjectID())
		blkID, rowOffset := rowID.Decode()
		blkOffset := blkID.Sequence()
		id.SetBlockOffset(blkOffset)
		// search the read set to check wether the delete node relevant
		// block was deleted.
		// if not deleted, go to next
		// if deleted, try to transfer the delete node
		if err = tbl.store.warChecker.checkOne(
			id,
			ts,
			phase == txnif.PrePreparePhase,
		); err == nil {
			continue
		}

		// if the error is not a r-w conflict. something wrong really happened
		if !moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict) {
			return
		}
		transferd.Add(uint64(i))
		tbl.store.warChecker.Delete(id)
		pk := pkVec.Get(i)
		if pkType == nil {
			pkType = pkVec.GetType()
		}

		// try to transfer the delete node
		// here are some possible returns
		// nil: transferred successfully
		// ErrTxnRWConflict: the target block was also be compacted
		// ErrTxnWWConflict: w-w error
		if _, err = tbl.TransferDeleteRows(id, rowOffset, pk, pkType, phase, ts); err != nil {
			return
		}
	}
	if transferd.IsEmpty() {
		return
	}
	for i, attr := range deletes.Attrs {
		// Skip the rowid column.
		// The rowid column is always empty in the delete node.
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		deletes.Vecs[i].CompactByBitmap(&transferd)
	}
	return
}

// recurTransferDelete recursively transfer the deletes to the target block.
// memo stores the pined transfer hash page for deleted and committed blocks.
// id is the deleted and committed block to transfer
func (tbl *txnTable) recurTransferDelete(
	memo map[types.Blockid]*common.PinnedItem[*model.TransferHashPage],
	page *model.TransferHashPage,
	id *common.ID, // the block had been deleted and committed.
	row uint32,
	pk any,
	pkType *types.Type,
	depth int,
	ts types.TS,
	phase string,
) error {

	var page2 *common.PinnedItem[*model.TransferHashPage]

	rowID, ok := page.Transfer(row)
	if !ok {
		err := moerr.NewTxnWWConflictNoCtx(0, "")
		msg := fmt.Sprintf("table-%d blk-%d delete row-%d depth-%d",
			id.TableID,
			id.BlockID,
			row,
			depth)
		logutil.Warnf("[ts=%s]TransferDeleteNode: %v",
			tbl.store.txn.GetStartTS().ToString(),
			msg)
		return err
	}
	blockID, offset := rowID.Decode()
	newID := &common.ID{
		DbID:    id.DbID,
		TableID: id.TableID,
		BlockID: *blockID,
	}

	//check if the target block had been soft deleted and committed before ts,
	//if not, transfer the deletes to the target block,
	//otherwise recursively transfer the deletes to the next target block.
	obj, err := tbl.store.warChecker.CacheGet(newID.DbID, newID.TableID, newID.ObjectID(), false)
	if err != nil {
		return err
	}
	err = readWriteConfilictCheck(
		obj,
		ts,
		phase == txnif.PrePreparePhase,
	)
	if err == nil {
		pkVec := tbl.store.rt.VectorPool.Small.GetVector(pkType)
		pkVec.Append(pk, false)
		defer pkVec.Close()
		rowIDVec := tbl.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		rowID := types.NewRowid(&newID.BlockID, offset)
		rowIDVec.Append(rowID, false)
		defer rowIDVec.Close()
		//transfer the deletes to the target block.
		if err = tbl.DeleteByPhyAddrKeys(
			rowIDVec, pkVec, handle.DT_Normal); err != nil {
			return err
		}
		common.DoIfDebugEnabled(func() {
			logutil.Infof("depth-%d %s transfer delete from blk-%s row-%d to blk-%s row-%d, txn %x, val %v",
				depth,
				tbl.dataTable.schema.Name,
				id.BlockID.String(),
				row,
				blockID.String(),
				offset,
				tbl.store.txn.GetID(),
				pk)
		})
		return nil
	}
	tbl.store.warChecker.conflictSet[*newID.ObjectID()] = true
	//prepare for recursively transfer the deletes to the next target block.
	if page2, ok = memo[*blockID]; !ok {
		page2, err = tbl.store.rt.TransferTable.Pin(*newID)
		if err != nil {
			return err
		}
		memo[*blockID] = page2
	}
	newID = &common.ID{
		DbID:    id.DbID,
		TableID: id.TableID,
		BlockID: *blockID,
	}
	//caudal recursion
	return tbl.recurTransferDelete(
		memo,
		page2.Item(),
		newID,
		offset,
		pk,
		pkType,
		depth+1,
		ts,
		phase)
}

func (tbl *txnTable) TransferDeleteRows(
	id *common.ID,
	row uint32,
	pk any,
	pkType *types.Type,
	phase string,
	ts types.TS,
) (transferred bool, err error) {
	memo := make(map[types.Blockid]*common.PinnedItem[*model.TransferHashPage])
	common.DoIfDebugEnabled(func() {
		logutil.Info("[Start]",
			common.AnyField("txn-ctx", tbl.store.txn.Repr()),
			common.OperationField("transfer-deletes"),
			common.OperandField(id.BlockString()),
			common.AnyField("phase", phase))
	})
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Info("[End]",
				common.AnyField("txn-ctx", tbl.store.txn.Repr()),
				common.OperationField("transfer-deletes"),
				common.OperandField(id.BlockString()),
				common.AnyField("phase", phase),
				common.ErrorField(err))
		})
		for _, m := range memo {
			m.Close()
		}
	}()

	pinned, err := tbl.store.rt.TransferTable.Pin(*id)
	// cannot find a transferred record. maybe the transferred record was TTL'ed
	// here we can convert the error back to r-w conflict
	if err != nil {
		oldErr := err
		err = moerr.NewTxnRWConflictNoCtx()
		logutil.Error(
			"TRANSFER-ERR",
			zap.Error(err),
			zap.String("old-err", oldErr.Error()),
			zap.String("id", id.String()),
			zap.String("txn", tbl.store.txn.String()),
			zap.String("phase", phase),
		)

		return
	}
	memo[id.BlockID] = pinned

	// logutil.Infof("TransferDeleteNode deletenode %s", node.DeleteNode.(*updates.DeleteNode).GeneralVerboseString())
	page := pinned.Item()
	depth := 0
	if err = tbl.recurTransferDelete(
		memo, page, id, row, pk, pkType, depth, ts, phase); err != nil {
		return
	}

	return
}

func (tbl *txnTable) WaitSynced() {
	for _, e := range tbl.logs {
		if err := e.WaitDone(); err != nil {
			panic(err)
		}
		e.Free()
	}
}

func (tbl *txnTable) CollectCmd(cmdMgr *commandManager) (err error) {
	tbl.csnStart = uint32(cmdMgr.GetCSN())
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		csn := cmdMgr.GetCSN()
		cmd, err := txnEntry.MakeCommand(csn)
		// logutil.Infof("%d-%d",csn,cmd.GetType())
		if err != nil {
			return err
		}
		if cmd == nil {
			panic(txnEntry)
		}
		cmdMgr.AddCmd(cmd)
	}
	err = tbl.dataTable.collectCmd(cmdMgr)
	if err != nil {
		return
	}
	if tbl.tombstoneTable != nil {
		err = tbl.tombstoneTable.collectCmd(cmdMgr)
	}
	return
}

func (tbl *txnTable) GetObject(id *types.Objectid, isTombstone bool) (obj handle.Object, err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		tbl.entry.ID,
		id,
		isTombstone)
	if err != nil {
		return
	}
	obj = buildObject(tbl, meta)
	return
}

func (tbl *txnTable) SoftDeleteObject(id *types.Objectid, isTombstone bool) (err error) {
	txnEntry, err := tbl.entry.DropObjectEntry(
		id, tbl.store.txn, isTombstone)
	if err != nil {
		return
	}
	if err = tbl.store.IncreateWriteCnt("soft delete object"); err != nil {
		return
	}
	if txnEntry != nil {
		tbl.txnEntries.Append(txnEntry)
	}
	tbl.store.txn.GetMemo().AddObject(
		tbl.entry.GetDB().GetID(), tbl.entry.ID, id, isTombstone)
	return
}

func (tbl *txnTable) CreateObject(isTombstone bool) (obj handle.Object, err error) {
	perfcounter.Update(tbl.store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.Create.Add(1)
	})
	sorted := isTombstone
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(
		&noid,
		true,
		sorted,
		false,
	)
	return tbl.createObject(
		&objectio.CreateObjOpt{Stats: stats, IsTombstone: isTombstone},
	)
}

func (tbl *txnTable) CreateNonAppendableObject(opts *objectio.CreateObjOpt) (obj handle.Object, err error) {
	perfcounter.Update(
		tbl.store.ctx,
		func(counter *perfcounter.CounterSet) {
			counter.TAE.Object.CreateNonAppendable.Add(1)
		})
	return tbl.createObject(opts)
}

func (tbl *txnTable) createObject(opts *objectio.CreateObjOpt) (obj handle.Object, err error) {
	var factory catalog.ObjectDataFactory
	if tbl.store.catalog.DataFactory != nil {
		factory = tbl.store.catalog.DataFactory.MakeObjectFactory()
	}
	var meta *catalog.ObjectEntry
	if meta, err = tbl.entry.CreateObject(
		tbl.store.txn, opts, factory); err != nil {
		return
	}
	obj = newObject(tbl, meta)
	if err = tbl.store.IncreateWriteCnt("create object"); err != nil {
		return
	}
	tbl.store.txn.GetMemo().AddObject(
		tbl.entry.GetDB().ID, tbl.entry.ID, meta.ID(), opts.IsTombstone)
	tbl.txnEntries.Append(meta)
	return
}

func (tbl *txnTable) LogTxnEntry(entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
	if err = tbl.store.IncreateWriteCnt("log txn entry"); err != nil {
		return
	}
	tbl.txnEntries.Append(entry)
	for _, id := range readedObject {
		// warChecker skip non-block read
		if objectio.IsEmptyBlkid(&id.BlockID) {
			continue
		}

		// record block into read set
		tbl.store.warChecker.InsertByID(
			tbl.entry.GetDB().ID,
			id.TableID,
			id.ObjectID(),
			false)
	}
	for _, id := range readedTombstone {
		// warChecker skip non-block read
		if objectio.IsEmptyBlkid(&id.BlockID) {
			continue
		}

		// record block into read set
		tbl.store.warChecker.InsertByID(
			tbl.entry.GetDB().ID,
			id.TableID,
			id.ObjectID(),
			true)
	}
	return
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) (err error) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	if err = tbl.store.IncreateWriteCnt("set create entry"); err != nil {
		return
	}
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.createEntry = e
	tbl.txnEntries.Append(e)
	return
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) (err error) {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	if err = tbl.store.IncreateWriteCnt("set drop entry"); err != nil {
		return
	}
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.dropEntry = e
	tbl.txnEntries.Append(e)
	return
}

func (tbl *txnTable) IsDeleted() bool {
	return tbl.dropEntry != nil
}

// GetLocalSchema returns the schema remains in the txn table, rather than the
// latest schema in TableEntry
func (tbl *txnTable) GetLocalSchema(isTombstone bool) *catalog.Schema {
	if isTombstone {
		if tbl.tombstoneTable == nil {
			tbl.tombstoneTable = newBaseTable(tbl.entry.GetLastestSchema(true), true, tbl)
		}
		return tbl.tombstoneTable.schema
	}
	return tbl.dataTable.schema
}

func (tbl *txnTable) GetMeta() *catalog.TableEntry {
	return tbl.entry
}

func (tbl *txnTable) GetID() uint64 {
	return tbl.entry.GetID()
}

func (tbl *txnTable) Close() error {
	var err error
	err = tbl.dataTable.Close()
	if err != nil {
		return err
	}
	if tbl.tombstoneTable != nil {
		err = tbl.tombstoneTable.Close()
		if err != nil {
			return err
		}
	}
	tbl.logs = nil
	tbl.txnEntries = nil
	return nil
}
func (tbl *txnTable) dedup(ctx context.Context, pk containers.Vector, isTombstone bool) (err error) {
	dedupType := tbl.store.txn.GetDedupType()
	if !dedupType.SkipWorkSpace() {
		//do PK deduplication check against txn's work space.
		if err = tbl.DedupWorkSpace(
			pk, isTombstone); err != nil {
			return
		}
	}
	if dedupType.SkipTargetAllCommitted() {
		return
	}
	// Incremental dedup requires transfer. It dedup when freeze.
	if !dedupType.SkipTargetOldCommitted() {
		if err = tbl.DedupSnapByPK(
			ctx,
			pk,
			isTombstone,
		); err != nil {
			return
		}
	}
	return
}
func (tbl *txnTable) Append(ctx context.Context, data *containers.Batch) (err error) {
	schema := tbl.dataTable.schema
	if schema.HasPK() && !schema.IsSecondaryIndexTable() {
		err = tbl.dedup(ctx, data.Vecs[schema.GetSingleSortKeyIdx()], false)
		if err != nil {
			return err
		}
	}
	if tbl.dataTable.tableSpace == nil {
		tbl.dataTable.tableSpace = newTableSpace(tbl, false)
	}
	_, err = tbl.dataTable.tableSpace.Append(data)
	return
}
func (tbl *txnTable) AddDataFiles(ctx context.Context, stats containers.Vector) (err error) {
	return stats.Foreach(func(v any, isNull bool, row int) error {
		s := objectio.ObjectStats(v.([]byte))
		return tbl.addObjsWithMetaLoc(ctx, s, false)
	}, nil)
}
func (tbl *txnTable) addObjsWithMetaLoc(
	ctx context.Context,
	stats objectio.ObjectStats,
	isTombstone bool,
) (err error) {
	if isTombstone {
		if tbl.tombstoneTable == nil {
			tbl.tombstoneTable = newBaseTable(tbl.entry.GetLastestSchema(true), true, tbl)
		}
		return tbl.tombstoneTable.addObjsWithMetaLoc(ctx, stats)
	} else {
		return tbl.dataTable.addObjsWithMetaLoc(ctx, stats)
	}
}

func (tbl *txnTable) GetByFilter(
	ctx context.Context, filter *handle.Filter,
) (id *common.ID, offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if tbl.dataTable.tableSpace != nil {
		id, offset, err = tbl.dataTable.tableSpace.GetByFilter(filter)
		if err == nil {
			return
		}
		err = nil
	}
	pkType := &tbl.dataTable.schema.GetPrimaryKey().Type
	pks := tbl.store.rt.VectorPool.Small.GetVector(pkType)
	defer pks.Close()
	pks.Append(filter.Val, false)
	rowIDs, err := tbl.dataTable.getRowsByPK(ctx, pks)
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
		return
	}
	defer rowIDs.Close()
	if rowIDs.IsNull(0) {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	err = tbl.findDeletes(tbl.store.ctx, rowIDs, types.TS{}, types.MaxTs())
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
		return
	}
	if rowIDs.IsNull(0) {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	rowID := vector.GetFixedAtNoTypeCheck[types.Rowid](rowIDs.GetDownstreamVector(), 0)
	id = tbl.entry.AsCommonID()
	id.BlockID = *rowID.BorrowBlockID()
	offset = rowID.GetRowOffset()
	var deleted bool
	deleted, err = tbl.IsDeletedInWorkSpace(&id.BlockID, offset)
	if err != nil {
		return
	}
	if deleted {
		id = nil
		err = moerr.NewNotFoundNoCtx()
	}
	return
}

func (tbl *txnTable) GetValue(
	ctx context.Context, id *common.ID, row uint32, col uint16, skipCheckDelete bool,
) (v any, isNull bool, err error) {
	if tbl.dataTable.tableSpace != nil && id.ObjectID().EQ(tbl.dataTable.tableSpace.entry.ID()) {
		return tbl.dataTable.tableSpace.GetValue(row, col)
	}
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(), false)
	if err != nil {
		panic(err)
	}
	block := meta.GetObjectData()
	_, blkIdx := id.BlockID.Offsets()
	return block.GetValue(
		ctx,
		tbl.store.txn,
		tbl.GetLocalSchema(false),
		blkIdx,
		int(row),
		int(col),
		skipCheckDelete,
		common.WorkspaceAllocator,
	)
}
func (tbl *txnTable) UpdateObjectStats(
	id *common.ID, stats *objectio.ObjectStats, isTombstone bool,
) error {
	meta, err := tbl.entry.GetObjectByID(id.ObjectID(), isTombstone)
	if err != nil {
		return err
	}
	isNewNode, err := meta.UpdateObjectInfo(tbl.store.txn, stats)
	if err != nil {
		return err
	}
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().ID, tbl.entry.ID, meta.ID(), isTombstone)
	if isNewNode {
		tbl.txnEntries.Append(meta)
	}
	return nil
}

func (tbl *txnTable) AlterTable(ctx context.Context, req *apipb.AlterTableReq) error {
	switch req.Kind {
	case apipb.AlterKind_UpdateConstraint,
		apipb.AlterKind_UpdateComment,
		apipb.AlterKind_AddColumn,
		apipb.AlterKind_DropColumn,
		apipb.AlterKind_RenameTable,
		apipb.AlterKind_UpdatePolicy,
		apipb.AlterKind_AddPartition,
		apipb.AlterKind_RenameColumn:
	case apipb.AlterKind_ReplaceDef:
		return nil
	default:
		return moerr.NewNYIf(ctx, "alter table %s", req.Kind.String())
	}
	if err := tbl.store.IncreateWriteCnt("alter table"); err != nil {
		return err
	}
	tbl.store.txn.GetMemo().AddCatalogChange()
	isNewNode, newSchema, err := tbl.entry.AlterTable(ctx, tbl.store.txn, req)
	if isNewNode {
		tbl.txnEntries.Append(tbl.entry)
	}
	if err != nil {
		return err
	}
	if req.Kind == apipb.AlterKind_RenameTable {
		rename := req.GetRenameTable()
		// udpate name index in db entry
		tenantID := newSchema.AcInfo.TenantID
		err = tbl.entry.GetDB().RenameTableInTxn(rename.OldName, rename.NewName, tbl.entry.ID, tenantID, tbl.store.txn, isNewNode)
		if err != nil {
			return err
		}
	}

	tbl.dataTable.schema = newSchema // update new schema to txn local schema
	//TODO(aptend): handle written data in localobj, keep the batch aligned with the new schema
	return err
}

// PrePrepareDedup do deduplication check for 1PC Commit or 2PC Prepare
func (tbl *txnTable) PrePrepareDedup(ctx context.Context, isTombstone bool, phase string, ts types.TS) (err error) {
	baseTable := tbl.getBaseTable(isTombstone)
	if baseTable == nil || baseTable.tableSpace == nil || !baseTable.schema.HasPK() || baseTable.schema.IsSecondaryIndexTable() {
		return
	}
	var zm index.ZM
	dedupType := tbl.store.txn.GetDedupType()
	if !dedupType.SkipSourcePersisted() {
		for _, stats := range baseTable.tableSpace.stats {
			err = tbl.DoPrecommitDedupByNode(ctx, stats, isTombstone)
			if err != nil {
				return
			}
		}
	}

	if baseTable.tableSpace.node == nil {
		return
	}
	node := baseTable.tableSpace.node
	pkColPos := baseTable.schema.GetSingleSortKeyIdx()
	pkVec, err := node.WindowColumn(0, node.Rows(), pkColPos)
	if err != nil {
		return err
	}
	if zm.Valid() {
		zm.ResetMinMax()
	} else {
		pkType := pkVec.GetType()
		zm = index.NewZM(pkType.Oid, pkType.Scale)
	}
	if err = index.BatchUpdateZM(zm, pkVec.GetDownstreamVector()); err != nil {
		pkVec.Close()
		return err
	}
	if err = tbl.DoPrecommitDedupByPK(pkVec, zm, isTombstone, phase, ts); err != nil {
		pkVec.Close()
		return err
	}
	pkVec.Close()
	tbl.dedupTS = ts
	return
}

// DedupSnapByPK 1. checks whether these primary keys exist in the list of block
// which are visible and not dropped at txn's snapshot timestamp.
// 2. It is called when appending data into this table.
func (tbl *txnTable) DedupSnapByPK(
	ctx context.Context,
	keys containers.Vector,
	isTombstone bool,
) (err error) {
	r := trace.StartRegion(ctx, "DedupSnapByPK")
	defer r.End()
	var rowIDs containers.Vector
	rowIDs, err = tbl.getBaseTable(isTombstone).getRowsByPK(ctx, keys)
	if err != nil {
		logutil.Errorf("getRowsByPK failed, %v", err)
		return
	}
	defer rowIDs.Close()
	from, to := types.TS{}, tbl.store.txn.GetStartTS()
	if !isTombstone {
		err = tbl.findDeletes(ctx, rowIDs, from, to)
		if err != nil {
			logutil.Errorf("getRowsByPK failed 2, %v", err)
			return
		}
	}
	for i := 0; i < rowIDs.Length(); i++ {
		colName := tbl.getBaseTable(isTombstone).schema.GetPrimaryKey().Name
		if !rowIDs.IsNull(i) {
			logutil.Error("Append Duplicate",
				zap.String("table", tbl.dataTable.schema.Name),
				zap.Bool("isTombstone", isTombstone),
				zap.String("pk", keys.PPString(keys.Length())),
				zap.String("rowids", rowIDs.PPString(rowIDs.Length())),
			)
			entry := common.TypeStringValue(*keys.GetType(), keys.Get(i), false)
			return moerr.NewDuplicateEntryNoCtx(entry, colName)
		}
	}
	return
}

/*
findDeletes set the rowIDs to null if the row is deleted, and committed in time range [from, to]

candidates:
1. NAppendable where from <= createdAt <= to
2. Appendable where x <= createdAt <= to,  where x is the first appendable entry with CreatedAt < from
*/
func (tbl *txnTable) findDeletes(
	ctx context.Context,
	rowIDs containers.Vector,
	from, to types.TS,
) (err error) {
	pkType := rowIDs.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, rowIDs.GetDownstreamVector()); err != nil {
		return
	}
	tbl.contains(ctx, rowIDs, keysZM, common.WorkspaceAllocator)

	tbl.entry.WaitTombstoneObjectCommitted(to)
	it := tbl.entry.MakeTombstoneObjectIt()
	defer it.Release()
	var earlybreak bool
	for ok := it.Last(); ok; ok = it.Prev() {
		if earlybreak {
			break
		}
		obj := it.Item()

		if obj.CreatedAt.GT(&to) {
			continue
		}

		if obj.IsAppendable() {
			if !obj.HasDropIntent() && obj.CreatedAt.LT(&from) {
				earlybreak = true
			}
		} else if obj.CreatedAt.LT(&from) {
			continue
		}

		// only keep the category-a + category-c for candidates.
		if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil {
			continue
		}

		if !obj.VisibleByTS(to) {
			continue
		}
		objData := obj.GetObjectData()
		if objData == nil {
			panic(fmt.Sprintf("logic error, object %v", obj.StringWithLevel(3)))
		}
		// PXU TODO: jxm need to double check this logic
		// if !obj.ObjectLocation().IsEmpty() {
		if obj.Rows() != 0 {
			var skip bool
			if skip, err = quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				continue
			}
		}

		if err = objData.Contains(
			ctx,
			tbl.store.txn,
			rowIDs,
			keysZM,
			common.WorkspaceAllocator,
		); err != nil {
			// logutil.Infof("%s, %s, %v", obj.String(), rowmask, err)
			return
		}
	}
	return
}

// DoPrecommitDedupByPK 1. it do deduplication by traversing all the Objects/blocks, and
// skipping over some blocks/Objects which being active or drop-committed or aborted;
//  2. it is called when txn dequeues from preparing queue.
//  3. we should make this function run quickly as soon as possible.
//     TODO::it would be used to do deduplication with the logtail.
func (tbl *txnTable) DoPrecommitDedupByPK(
	pks containers.Vector,
	pksZM index.ZM,
	isTombstone bool,
	phase string,
	ts types.TS,
) (err error) {
	moprobe.WithRegion(context.Background(), moprobe.TxnTableDoPrecommitDedupByPK, func() {
		now := tbl.store.rt.Now()
		if tbl.dedupTS.IsEmpty() {
			tbl.dedupTS = tbl.store.txn.GetStartTS()
		}
		var rowIDs containers.Vector
		rowIDs, err = tbl.getBaseTable(isTombstone).incrementalGetRowsByPK(tbl.store.ctx, pks, tbl.dedupTS.Next(), ts, phase == txnif.PrePreparePhase)
		if err != nil {
			return
		}
		defer rowIDs.Close()
		if !isTombstone {
			err = tbl.findDeletes(tbl.store.ctx, rowIDs, tbl.dedupTS.Next(), now)
			if err != nil {
				return
			}
		}
		for i := 0; i < rowIDs.Length(); i++ {
			var colName string
			if isTombstone {
				colName = tbl.tombstoneTable.schema.GetPrimaryKey().Name
			} else {
				colName = tbl.dataTable.schema.GetPrimaryKey().Name
			}
			if !rowIDs.IsNull(i) {
				logutil.Error("Duplicate",
					zap.String("table", tbl.dataTable.schema.Name),
					zap.Bool("is tombstone", isTombstone),
					zap.String("phase", phase),
					zap.String("from", tbl.dedupTS.Next().ToString()),
					zap.String("to", ts.ToString()),
				)
				entry := common.TypeStringValue(*pks.GetType(), pks.Get(i), false)
				err = moerr.NewDuplicateEntryNoCtx(entry, colName)
				return
			}
		}
	})
	return
}

func (tbl *txnTable) DoPrecommitDedupByNode(ctx context.Context, stats objectio.ObjectStats, isTombstone bool) (err error) {
	//loaded := false
	//TODO::load ZM/BF index first, then load PK column if necessary.

	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	schema := tbl.getBaseTable(isTombstone).schema
	blkMaxRows := schema.Extra.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}

	for _, loc := range metaLocs {
		var vectors []containers.Vector
		var closeFunc func()
		vectors, closeFunc, err = ioutil.LoadColumns2(
			ctx,
			[]uint16{uint16(schema.GetSingleSortKeyIdx())},
			nil,
			tbl.store.rt.Fs,
			loc,
			fileservice.Policy(0),
			false,
			nil,
		)
		if err != nil {
			return err
		}
		pks := vectors[0]
		defer closeFunc()
		defer pks.Close()
		var rowIDs containers.Vector
		now := tbl.store.rt.Now()
		if tbl.dedupTS.IsEmpty() {
			tbl.dedupTS = tbl.store.txn.GetStartTS()
		}
		rowIDs, err = tbl.getBaseTable(isTombstone).incrementalGetRowsByPK(ctx, pks, tbl.dedupTS, now, true)
		if err != nil {
			return
		}
		defer rowIDs.Close()
		if !isTombstone {
			err = tbl.findDeletes(ctx, rowIDs, tbl.dedupTS, now)
		}
		if err != nil {
			return
		}
		for i := 0; i < rowIDs.Length(); i++ {
			if !rowIDs.IsNull(i) {
				colName := tbl.getSchema(false).GetPrimaryKey().Name
				entry := common.TypeStringValue(*pks.GetType(), pks.Get(i), false)
				err = moerr.NewDuplicateEntryNoCtx(entry, colName)
				return
			}
		}
	}
	return
}
func (tbl *txnTable) getSchema(isTombstone bool) *catalog.Schema {
	if isTombstone {
		return tbl.tombstoneTable.schema
	} else {
		return tbl.dataTable.schema
	}
}
func (tbl *txnTable) DedupWorkSpace(key containers.Vector, isTombstone bool) (err error) {
	if tbl.getBaseTable(isTombstone) == nil {
		return nil
	}
	index := NewSimpleTableIndex()
	//Check whether primary key is duplicated.
	if err = index.BatchInsert(
		tbl.getSchema(isTombstone).GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		return
	}

	if isTombstone {
		return tbl.tombstoneTable.DedupWorkSpace(key)
	} else {
		return tbl.dataTable.DedupWorkSpace(key)
	}
}

func (tbl *txnTable) DoBatchDedup(key containers.Vector) (err error) {
	index := NewSimpleTableIndex()
	//Check whether primary key is duplicated.
	if err = index.BatchInsert(
		tbl.dataTable.schema.GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		logutil.Infof("DoBatchDedup BatchInsert failed1 %v", err)
		return
	}

	err = tbl.DedupWorkSpace(key, false)
	if err != nil {
		logutil.Infof("DoBatchDedup BatchInsert failed2 %v", err)
		return
	}
	//Check whether primary key is duplicated in txn's snapshot data.
	err = tbl.DedupSnapByPK(context.Background(), key, false)
	return
}

func (tbl *txnTable) BatchDedupLocal(bat *containers.Batch) (err error) {
	err = tbl.dataTable.BatchDedupLocal(bat)
	return
}

func (tbl *txnTable) PrepareRollback() (err error) {
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = txnEntry.PrepareRollback(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) ApplyAppend() (err error) {
	if tbl.dataTable.tableSpace != nil {
		err = tbl.dataTable.tableSpace.ApplyAppend()
	}
	if err != nil {
		return
	}
	if tbl.tombstoneTable != nil && tbl.tombstoneTable.tableSpace != nil {
		err = tbl.tombstoneTable.tableSpace.ApplyAppend()
	}
	return
}

func (tbl *txnTable) PrePrepare() (err error) {
	err = tbl.dataTable.PrePrepare()
	if err != nil {
		return
	}
	if tbl.tombstoneTable != nil {
		err = tbl.tombstoneTable.PrePrepare()
	}
	return
}

func (tbl *txnTable) dumpCore(errMsg string) {
	var errInfo bytes.Buffer
	errInfo.WriteString(fmt.Sprintf("Table: %s", tbl.entry.String()))
	errInfo.WriteString(fmt.Sprintf("\nTxn: %s", tbl.store.txn.String()))
	errInfo.WriteString(fmt.Sprintf("\nErr: %s", errMsg))
	logutil.Error(errInfo.String())
	util.EnableCoreDump()
	util.CoreDump()
}

func (tbl *txnTable) PrepareCommit() (err error) {
	nodeCount := len(tbl.txnEntries.entries)
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.PrepareCommit(); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrTxnNotFound) {
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%d/%d No Txn, node type %T, ", idx, len(tbl.txnEntries.entries), node))
				obj, ok := node.(*catalog.ObjectEntry)
				if ok {
					buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
				}
				for idx2, node2 := range tbl.txnEntries.entries {
					buf.WriteString(fmt.Sprintf("%d. node type %T, ", idx2, node2))
					obj, ok := node2.(*catalog.ObjectEntry)
					if ok {
						buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
					}
				}
				tbl.dumpCore(buf.String())
			}
			break
		}
	}
	// In flush and merge, it transfers deletes when prepare commit.
	// It may adds new txn entries.
	// Prepare commit them, if the length of tbl.txnEntries.entries changes.
	if len(tbl.txnEntries.entries) != nodeCount {
		for idx := nodeCount; idx < len(tbl.txnEntries.entries); idx++ {
			if tbl.txnEntries.IsDeleted(idx) {
				continue
			}
			if err = tbl.txnEntries.entries[idx].PrepareCommit(); err != nil {
				break
			}
		}
	}
	return
}

func (tbl *txnTable) PreApplyCommit() (err error) {
	return tbl.ApplyAppend()
}

func (tbl *txnTable) ApplyCommit() (err error) {
	csn := tbl.csnStart
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.ApplyCommit(tbl.store.txn.GetID()); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrTxnNotFound) {
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%d/%d No Txn, node type %T, ", idx, len(tbl.txnEntries.entries), node))
				obj, ok := node.(*catalog.ObjectEntry)
				if ok {
					buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
				}
				for idx2, node2 := range tbl.txnEntries.entries {
					buf.WriteString(fmt.Sprintf("%d. node type %T, ", idx2, node2))
					obj, ok := node2.(*catalog.ObjectEntry)
					if ok {
						buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
					}
				}
				tbl.dumpCore(buf.String())
			}
			if moerr.IsMoErrCode(err, moerr.ErrMissingTxn) {
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%d/%d missing txn, node type %T, ", idx, len(tbl.txnEntries.entries), node))
				obj, ok := node.(*catalog.ObjectEntry)
				if ok {
					buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
				}
				for idx2, node2 := range tbl.txnEntries.entries {
					buf.WriteString(fmt.Sprintf("%d. node type %T, ", idx2, node2))
					obj, ok := node2.(*catalog.ObjectEntry)
					if ok {
						buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
					}
				}
				tbl.dumpCore(buf.String())
			}
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) ApplyRollback() (err error) {
	csn := tbl.csnStart
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.ApplyRollback(); err != nil {
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) CleanUp() {
	tbl.dataTable.CleanUp()
	if tbl.tombstoneTable != nil {
		tbl.tombstoneTable.CleanUp()
	}
}

func (tbl *txnTable) RangeDeleteLocalRows(start, end uint32) (err error) {
	if tbl.dataTable.tableSpace != nil {
		err = tbl.dataTable.tableSpace.RangeDelete(start, end)
	}
	return
}

// for ut
// RangeDelete delete block rows in range [start, end]
func (tbl *txnTable) RangeDelete(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector,
	dt handle.DeleteType) (err error) {
	if tbl.dataTable.tableSpace != nil && id.ObjectID().EQ(tbl.dataTable.tableSpace.entry.ID()) {
		err = tbl.RangeDeleteLocalRows(start, end)
		return
	}
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), common.DebugAllocator)
	for i := start; i <= end; i++ {
		rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(*id.ObjectID(), id.BlockID.Sequence(), i)
		rowIDVec.Append(rowID, false)
	}
	return tbl.DeleteByPhyAddrKeys(rowIDVec, pk, dt)
}

func (tbl *txnTable) DeleteByPhyAddrKeys(
	rowIDVec containers.Vector,
	pk containers.Vector,
	dt handle.DeleteType) (err error) {
	var rowIDStr string
	defer func() {
		if err == nil {
			return
		}
		// if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
		// 	moerr.NewTxnWriteConflictNoCtx("table-%d blk-%d delete rows from %d to %d",
		// 		id.TableID,
		// 		id.BlockID,
		// 		start,
		// 		end)
		// }
		// This err also captured by txn's write conflict check.
		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			err = moerr.NewTxnWWConflictNoCtx(tbl.GetID(), pk.PPString(pk.Length()))
		}
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[ts=%s]: table-%d delete rows(%v) %v",
				tbl.store.txn.GetStartTS().ToString(),
				tbl.GetID(),
				rowIDStr,
				err)
		})

		if tbl.store.rt.Options.IncrementalDedup && moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			logutil.Warnf("[txn%X,ts=%s]: table-%d delete rows(%v) pk %s",
				tbl.store.txn.GetID(),
				tbl.store.txn.GetStartTS().ToString(),
				tbl.GetID(),
				rowIDStr,
				pk.PPString(pk.Length()),
			)
		}
	}()
	deleteBatch := tbl.createTombstoneBatch(rowIDVec, pk)
	defer func() {
		if err != nil {
			rowIDStr = rowIDVec.PPString(1)
		}
		for _, attr := range deleteBatch.Attrs {
			if attr == objectio.TombstoneAttr_PK_Attr {
				// not close pk
				continue
			}
			deleteBatch.GetVectorByName(attr).Close()
		}
	}()
	if tbl.tombstoneTable == nil {
		tbl.tombstoneTable = newBaseTable(tbl.entry.GetLastestSchema(true), true, tbl)
	}
	err = tbl.dedup(tbl.store.ctx, deleteBatch.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr), true)
	if err != nil {
		return
	}
	if tbl.tombstoneTable.tableSpace == nil {
		tbl.tombstoneTable.tableSpace = newTableSpace(tbl, true)
	}
	_, err = tbl.tombstoneTable.tableSpace.Append(deleteBatch)
	if err != nil {
		return
	}
	if dt == handle.DT_MergeCompact {
		anode := tbl.tombstoneTable.tableSpace.node
		anode.isMergeCompact = true
		if tbl.store.txn.GetTxnState(false) != txnif.TxnStateActive {
			startOffset := anode.data.Length() - deleteBatch.Length()
			tbl.tombstoneTable.tableSpace.prepareApplyANode(anode, uint32(startOffset))
		}
	}
	rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVec.GetDownstreamVector())
	for _, rowID := range rowIDs {
		obj, err := tbl.store.warChecker.CacheGet(
			tbl.entry.GetDB().ID,
			tbl.GetID(), rowID.BorrowObjectID(),
			false)
		if err != nil {
			return err
		}
		tbl.store.warChecker.Insert(obj)
	}
	return
}
func (tbl *txnTable) contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	mp *mpool.MPool,
) (err error) {
	if tbl.tombstoneTable == nil || tbl.tombstoneTable.tableSpace == nil {
		return
	}
	if tbl.tombstoneTable.tableSpace.node != nil {
		workspaceDeleteBatch := tbl.tombstoneTable.tableSpace.node.data
		keyRowids := vector.MustFixedColWithTypeCheck[types.Rowid](keys.GetDownstreamVector())
		workspaceRowids := vector.MustFixedColWithTypeCheck[types.Rowid](
			workspaceDeleteBatch.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector(),
		)
		for j := 0; j < keys.Length(); j++ {
			if keys.IsNull(j) {
				continue
			}
			for i := 0; i < workspaceDeleteBatch.Length(); i++ {
				if keyRowids[j] == workspaceRowids[i] {
					containers.UpdateValue(keys.GetDownstreamVector(), uint32(j), nil, true, mp)
				}
			}
		}
	}
	for _, stats := range tbl.tombstoneTable.tableSpace.stats {
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := tbl.tombstoneTable.schema.Extra.BlockMaxRows
		tombStoneZM := stats.SortKeyZoneMap()
		var skip bool
		if skip = !tombStoneZM.FastIntersect(keysZM); skip {
			continue
		}
		var bf objectio.BloomFilter
		bf, err = objectio.FastLoadBF(ctx, stats.ObjectLocation(), false, tbl.store.rt.Fs)
		if err != nil {
			return
		}
		idx := indexwrapper.NewImmutIndex(stats.SortKeyZoneMap(), bf, stats.ObjectLocation())
		for i := uint16(0); i < uint16(blkCount); i++ {
			sel, err := idx.BatchDedup(ctx, keys, keysZM, tbl.store.rt, true, uint32(i))
			if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
				continue
			}

			var blkRow uint32
			if totalRow > blkMaxRows {
				blkRow = blkMaxRows
			} else {
				blkRow = totalRow
			}
			totalRow -= blkRow
			metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

			vectors, closeFunc, err := ioutil.LoadColumns2(
				tbl.store.ctx,
				[]uint16{uint16(tbl.tombstoneTable.schema.GetSingleSortKeyIdx())},
				nil,
				tbl.store.rt.Fs,
				metaloc,
				fileservice.Policy(0),
				false,
				nil,
			)
			if err != nil {
				return err
			}
			data := vector.MustFixedColWithTypeCheck[types.Rowid](vectors[0].GetDownstreamVector())
			containers.ForeachVector(keys,
				func(id types.Rowid, isNull bool, row int) error {
					if keys.IsNull(row) {
						return nil
					}
					if _, existed := compute.GetOffsetWithFunc(
						data,
						id,
						types.CompareRowidRowidAligned,
						nil,
					); existed {
						keys.Update(row, nil, true)
					}
					return nil
				}, sel)
			closeFunc()
		}
	}
	return nil
}
func (tbl *txnTable) createTombstoneBatch(
	rowIDs containers.Vector,
	pk containers.Vector) *containers.Batch {
	if pk.Length() != rowIDs.Length() {
		panic(fmt.Sprintf("logic err, invalid pkVec length, pk length = %d, rowid length %d", pk.Length(), rowIDs.Length()))
	}
	bat := catalog.NewTombstoneBatchWithPKVector(pk, rowIDs, common.WorkspaceAllocator)
	return bat
}

func (tbl *txnTable) AddPersistedTombstoneFile(id *common.ID, stats objectio.ObjectStats) (ok bool, err error) {
	if tbl.tombstoneTable == nil {
		tbl.tombstoneTable = newBaseTable(tbl.entry.GetLastestSchema(true), true, tbl)
	}
	err = tbl.addObjsWithMetaLoc(tbl.store.ctx, stats, true)
	if err == nil {
		tbl.tombstoneTable.tableSpace.objs = append(tbl.tombstoneTable.tableSpace.objs, id.ObjectID())
		ok = true
	}
	return
}

func (tbl *txnTable) FillInWorkspaceDeletes(blkID types.Blockid, deletes **nulls.Nulls, deleteStartOffset uint64) error {
	if tbl.tombstoneTable == nil || tbl.tombstoneTable.tableSpace == nil {
		return nil
	}
	if tbl.tombstoneTable.tableSpace.node != nil {
		node := tbl.tombstoneTable.tableSpace.node
		rowVec := vector.MustFixedColWithTypeCheck[types.Rowid](
			node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector(),
		)
		for i := range rowVec {
			if *rowVec[i].BorrowBlockID() == blkID {
				row := rowVec[i].GetRowOffset()
				if *deletes == nil {
					*deletes = &nulls.Nulls{}
				}
				(*deletes).Add(uint64(row) + deleteStartOffset)
			}
		}
	}
	for _, stats := range tbl.tombstoneTable.tableSpace.stats {
		metaLocs := make([]objectio.Location, 0)
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := tbl.tombstoneTable.schema.Extra.BlockMaxRows
		for i := uint16(0); i < uint16(blkCount); i++ {
			var blkRow uint32
			if totalRow > blkMaxRows {
				blkRow = blkMaxRows
			} else {
				blkRow = totalRow
			}
			totalRow -= blkRow
			metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

			metaLocs = append(metaLocs, metaloc)
		}
		for _, loc := range metaLocs {
			vectors, closeFunc, err := ioutil.LoadColumns2(
				tbl.store.ctx,
				[]uint16{uint16(tbl.tombstoneTable.schema.GetSingleSortKeyIdx())},
				nil,
				tbl.store.rt.Fs,
				loc,
				fileservice.Policy(0),
				false,
				nil,
			)
			if err != nil {
				return err
			}
			rowids := vector.MustFixedColWithTypeCheck[types.Rowid](vectors[0].GetDownstreamVector())
			for i := 0; i < vectors[0].Length(); i++ {
				rowID := rowids[i]
				if *rowID.BorrowBlockID() == blkID {
					_, row := rowID.Decode()
					if *deletes == nil {
						*deletes = &nulls.Nulls{}
					}
					(*deletes).Add(uint64(row) + deleteStartOffset)
				}
			}
			closeFunc()
		}
	}
	return nil
}

func (tbl *txnTable) IsDeletedInWorkSpace(blkID *objectio.Blockid, row uint32) (bool, error) {
	if tbl.tombstoneTable == nil || tbl.tombstoneTable.tableSpace == nil {
		return false, nil
	}
	if tbl.tombstoneTable.tableSpace.node != nil {
		node := tbl.tombstoneTable.tableSpace.node
		rowids := vector.MustFixedColWithTypeCheck[types.Rowid](
			node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector(),
		)
		for i := 0; i < node.data.Length(); i++ {
			blk, rowOffset := rowids[i].Decode()
			if blk.EQ(blkID) && row == rowOffset {
				return true, nil
			}
		}
	}

	for _, stats := range tbl.tombstoneTable.tableSpace.stats {
		metaLocs := make([]objectio.Location, 0)
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := tbl.tombstoneTable.schema.Extra.BlockMaxRows
		for i := uint16(0); i < uint16(blkCount); i++ {
			var blkRow uint32
			if totalRow > blkMaxRows {
				blkRow = blkMaxRows
			} else {
				blkRow = totalRow
			}
			totalRow -= blkRow
			metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

			metaLocs = append(metaLocs, metaloc)
		}
		for _, loc := range metaLocs {
			vectors, closeFunc, err := ioutil.LoadColumns2(
				tbl.store.ctx,
				[]uint16{uint16(tbl.tombstoneTable.schema.GetSingleSortKeyIdx())},
				nil,
				tbl.store.rt.Fs,
				loc,
				fileservice.Policy(0),
				false,
				nil,
			)
			if err != nil {
				return false, err
			}
			defer closeFunc()
			rowids := vector.MustFixedColWithTypeCheck[types.Rowid](vectors[0].GetDownstreamVector())
			for i := 0; i < vectors[0].Length(); i++ {
				blk, rowOffset := rowids[i].Decode()
				if blk.EQ(blkID) && row == rowOffset {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
