// Copyright 2022 Matrix Origin
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

package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

var (
	readTxnErrors = map[uint16]struct{}{
		moerr.ErrTAERead:      {},
		moerr.ErrRpcError:     {},
		moerr.ErrWaitTxn:      {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
	writeTxnErrors = map[uint16]struct{}{
		moerr.ErrTAEWrite:     {},
		moerr.ErrRpcError:     {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
	commitTxnErrors = map[uint16]struct{}{
		moerr.ErrTAECommit:               {},
		moerr.ErrTAERollback:             {},
		moerr.ErrTAEPrepare:              {},
		moerr.ErrRpcError:                {},
		moerr.ErrTxnNotFound:             {},
		moerr.ErrTxnNotActive:            {},
		moerr.ErrLockTableBindChanged:    {},
		moerr.ErrCannotCommitOrphan:      {},
		moerr.ErrCannotCommitOnInvalidCN: {},
	}
	rollbackTxnErrors = map[uint16]struct{}{
		moerr.ErrTAERollback:  {},
		moerr.ErrRpcError:     {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
)

// WithUserTxn setup user transaction flag. Only user transactions need to be controlled for the maximum
// number of active transactions.
func WithUserTxn() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options = tc.opts.options.WithUserTxn()
	}
}

// WithTxnReadyOnly setup readonly flag
func WithTxnReadyOnly() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options = tc.opts.options.WithReadOnly()
	}
}

// WithTxnDisable1PCOpt disable 1pc opt on distributed transaction. By default, mo enables 1pc
// optimization for distributed transactions. For write operations, if all partitions' prepares are
// executed successfully, then the transaction is considered committed and returned directly to the
// client. Partitions' prepared data are committed asynchronously.
func WithTxnDisable1PCOpt() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options = tc.opts.options.WithDisable1PC()
	}
}

// WithTxnCNCoordinator set cn txn coordinator
func WithTxnCNCoordinator() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.coordinator = true
	}
}

// WithTxnLockService set txn lock service
func WithTxnLockService(lockService lockservice.LockService) TxnOption {
	return func(tc *txnOperator) {
		tc.lockService = lockService
	}
}

// WithTxnCreateBy set txn create by.
func WithTxnCreateBy(
	accountID uint32,
	userName string,
	sessionID string,
	connectionID uint32) TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options.CN = tc.sid
		tc.opts.options.SessionID = sessionID
		tc.opts.options.ConnectionID = connectionID
		tc.opts.options.AccountID = accountID
		tc.opts.options.UserName = userName
	}
}

// WithTxnCacheWrite Set cache write requests, after each Write call, the request will not be sent
// to the TN node immediately, but stored in the Coordinator's memory, and the Coordinator will
// choose the right time to send the cached requests. The following scenarios trigger the sending
// of requests to DN:
//  1. Before read, because the Coordinator is not aware of the format and content of the written data,
//     it is necessary to send the cached write requests to the corresponding TN node each time Read is
//     called, used to implement "read your write".
//  2. Before commit, obviously, the cached write requests needs to be sent to the corresponding TN node
//     before commit.
func WithTxnCacheWrite() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options = tc.opts.options.WithEnableCacheWrite()
		tc.mu.cachedWrites = make(map[uint64][]txn.TxnRequest)
	}
}

// WithSnapshotTS use a spec snapshot timestamp to build TxnOperator.
func WithSnapshotTS(ts timestamp.Timestamp) TxnOption {
	return func(tc *txnOperator) {
		tc.mu.txn.SnapshotTS = ts
	}
}

// WithTxnMode set txn mode
func WithTxnMode(value txn.TxnMode) TxnOption {
	return func(tc *txnOperator) {
		tc.mu.txn.Mode = value
	}
}

// WithTxnIsolation set txn isolation
func WithTxnIsolation(value txn.TxnIsolation) TxnOption {
	return func(tc *txnOperator) {
		tc.mu.txn.Isolation = value
	}
}

// WithTxnSkipLock skip txn lock on specified tables
func WithTxnSkipLock(
	tables []uint64,
	modes []lock.LockMode) TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options.SkipLockTables = append(tc.opts.options.SkipLockTables, tables...)
		tc.opts.options.SkipLockTableModes = append(tc.opts.options.SkipLockTableModes, modes...)
	}
}

// WithTxnEnableCheckDup enable check duplicate before commit to TN
func WithTxnEnableCheckDup() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options = tc.opts.options.WithEnableCheckDup()
	}
}

func WithDisableTrace(value bool) TxnOption {
	return func(tc *txnOperator) {
		if value {
			tc.opts.options = tc.opts.options.WithDisableTrace()
		}
	}
}

func WithDisableWaitPaused() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options = tc.opts.options.WithDisableWaitPaused()
	}
}

func WithSessionInfo(info string) TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options.SessionInfo = info
	}
}

func WithBeginAutoCommit(begin, autocommit bool) TxnOption {
	return func(tc *txnOperator) {
		tc.opts.options.ByBegin = begin
		tc.opts.options.Autocommit = autocommit
	}
}

func WithSkipPushClientReady() TxnOption {
	return func(tc *txnOperator) {
		tc.opts.skipWaitPushClient = true
	}
}

// WithTxnMode set txn mode
func WithWaitActiveHandle(fn func()) TxnOption {
	return func(tc *txnOperator) {
		tc.opts.waitActiveHandle = fn
	}
}

type txnOperator struct {
	sid             string
	logger          *log.MOLogger
	sender          rpc.TxnSender
	clock           clock.Clock
	lockService     lockservice.LockService
	timestampWaiter TimestampWaiter

	mu struct {
		sync.RWMutex
		waitActive   bool
		closed       bool
		txn          txn.TxnMeta
		cachedWrites map[uint64][]txn.TxnRequest
		lockTables   []lock.LockTable
		callbacks    map[EventType][]func(TxnEvent)
		retry        bool
		lockSeq      uint64
		waitLocks    map[uint64]Lock
		//read-only txn operators for supporting snapshot read feature.
		children []*txnOperator
		flag     uint32
	}

	reset struct {
		txnID                []byte
		parent               atomic.Pointer[txnOperator]
		waiter               *waiter
		waitActiveCost       time.Duration
		sequence             atomic.Uint64
		commitSeq            uint64
		createAt             time.Time
		commitAt             time.Time
		createTs             timestamp.Timestamp
		cannotCleanWorkspace bool
		workspace            Workspace
		commitCounter        counter
		rollbackCounter      counter
		runSqlCounter        counter
		incrStmtCounter      counter
		rollbackStmtCounter  counter
		fprints              footPrints
		runningSQL           atomic.Bool
		commitErr            error
	}

	opts struct {
		coordinator        bool
		skipWaitPushClient bool
		options            txn.TxnOptions
		waitActiveHandle   func()
	}
}

func newTxnOperator(
	sid string,
	clock clock.Clock,
	sender rpc.TxnSender,
	txnMeta txn.TxnMeta,
	options ...TxnOption,
) *txnOperator {
	tc := &txnOperator{
		sid:    sid,
		logger: util.GetLogger(sid),
		sender: sender,
		clock:  clock,
	}

	tc.init(txnMeta, options...)
	return tc
}

func (tc *txnOperator) init(
	txnMeta txn.TxnMeta,
	options ...TxnOption,
) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.initReset()
	tc.initProtectedFields()

	tc.mu.txn = txnMeta
	tc.reset.txnID = txnMeta.ID
	tc.reset.createAt = time.Now()
	tc.reset.createTs, _ = tc.clock.Now()
	for _, opt := range options {
		opt(tc)
	}
	tc.adjust()
	util.LogTxnCreated(tc.logger, tc.mu.txn)

	if tc.opts.options.UserTxn() {
		v2.TxnUserCounter.Inc()
	} else {
		v2.TxnInternalCounter.Inc()
	}
}

func (tc *txnOperator) initReset() {
	tc.reset.txnID = nil
	tc.reset.parent.Store(nil)
	tc.reset.waiter = nil
	tc.reset.waitActiveCost = 0
	tc.reset.sequence.Store(0)
	tc.reset.commitSeq = 0
	tc.reset.createAt = time.Time{}
	tc.reset.commitAt = time.Time{}
	tc.reset.createTs = timestamp.Timestamp{}
	tc.reset.cannotCleanWorkspace = false
	tc.reset.workspace = nil
	tc.reset.commitCounter = counter{}
	tc.reset.rollbackCounter = counter{}
	tc.reset.runSqlCounter = counter{}
	tc.reset.incrStmtCounter = counter{}
	tc.reset.rollbackStmtCounter = counter{}
	tc.reset.fprints = footPrints{}
	tc.reset.runningSQL.Store(false)
	tc.reset.commitErr = nil
}

func (tc *txnOperator) initProtectedFields() {
	tc.mu.waitActive = false
	tc.mu.closed = false
	tc.mu.retry = false
	tc.mu.lockSeq = 0
	tc.mu.txn = txn.TxnMeta{}
	tc.mu.lockTables = tc.mu.lockTables[:0]
	tc.mu.children = tc.mu.children[:0]
	if tc.mu.cachedWrites != nil {
		for k := range tc.mu.cachedWrites {
			delete(tc.mu.cachedWrites, k)
		}
	}
	if tc.mu.callbacks != nil {
		for k, v := range tc.mu.callbacks {
			tc.mu.callbacks[k] = v[:0]
		}
	}
	if tc.mu.waitLocks != nil {
		for k := range tc.mu.waitLocks {
			delete(tc.mu.waitLocks, k)
		}
	}
}

func (tc *txnOperator) IsSnapOp() bool {
	return tc.reset.parent.Load() != nil
}

func (tc *txnOperator) CloneSnapshotOp(snapshot timestamp.Timestamp) TxnOperator {
	op := &txnOperator{}
	op.mu.txn = txn.TxnMeta{
		SnapshotTS: snapshot,
		ID:         tc.mu.txn.ID,
		TNShards:   tc.mu.txn.TNShards,
	}
	op.reset.txnID = op.mu.txn.ID

	op.reset.workspace = tc.reset.workspace.CloneSnapshotWS()
	op.reset.workspace.BindTxnOp(op)
	op.logger = tc.logger
	op.sender = tc.sender
	op.timestampWaiter = tc.timestampWaiter

	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.children = append(tc.mu.children, op)

	op.reset.parent.Store(tc)
	return op
}

func newTxnOperatorWithSnapshot(
	logger *log.MOLogger,
	sender rpc.TxnSender,
	snapshot txn.CNTxnSnapshot,
) *txnOperator {
	tc := &txnOperator{sender: sender, logger: logger}
	tc.reset.txnID = snapshot.Txn.ID
	tc.opts.options = snapshot.Options
	tc.mu.txn = snapshot.Txn
	tc.mu.txn.Mirror = true
	tc.mu.lockTables = snapshot.LockTables
	tc.mu.flag = snapshot.Flag

	tc.adjust()
	util.LogTxnCreated(tc.logger, tc.mu.txn)
	return tc
}

func (tc *txnOperator) waitActive(ctx context.Context) error {
	if tc.reset.waiter == nil {
		return nil
	}

	tc.setWaitActive(true)
	if tc.opts.waitActiveHandle != nil {
		tc.opts.waitActiveHandle()
	}

	defer func() {
		tc.reset.waiter.close()
		tc.setWaitActive(false)
	}()

	cost, err := tc.doCostAction(
		time.Time{},
		WaitActiveEvent,
		func() error {
			return tc.reset.waiter.wait(ctx)
		},
		false)
	tc.reset.waitActiveCost = cost
	v2.TxnWaitActiveDurationHistogram.Observe(cost.Seconds())
	return err
}

func (tc *txnOperator) GetWaitActiveCost() time.Duration {
	return tc.reset.waitActiveCost
}

func (tc *txnOperator) notifyActive() {
	if tc.reset.waiter == nil {
		panic("BUG: notify active on non-waiter txn operator")
	}
	defer tc.reset.waiter.close()
	tc.reset.waiter.notify()
}

func (tc *txnOperator) AddWorkspace(workspace Workspace) {
	tc.reset.workspace = workspace
}

func (tc *txnOperator) GetWorkspace() Workspace {
	return tc.reset.workspace
}

func (tc *txnOperator) adjust() {
	if tc.sender == nil {
		tc.logger.Fatal("missing txn sender")
	}
	if len(tc.mu.txn.ID) == 0 {
		tc.logger.Fatal("missing txn id")
	}
	if tc.opts.options.ReadOnly() && tc.opts.options.CacheWriteEnabled() {
		tc.logger.Fatal("readyOnly and delayWrites cannot both be set")
	}
}

func (tc *txnOperator) Txn() txn.TxnMeta {
	return tc.getTxnMeta(false)
}

func (tc *txnOperator) TxnRef() *txn.TxnMeta {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return &tc.mu.txn
}

func (tc *txnOperator) SnapshotTS() timestamp.Timestamp {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.mu.txn.SnapshotTS
}

func (tc *txnOperator) CreateTS() timestamp.Timestamp {
	return tc.reset.createTs
}

func (tc *txnOperator) Status() txn.TxnStatus {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.mu.txn.Status
}

func (tc *txnOperator) Snapshot() (txn.CNTxnSnapshot, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkStatus(true); err != nil {
		return txn.CNTxnSnapshot{}, err
	}
	return txn.CNTxnSnapshot{
		Txn:        tc.mu.txn,
		LockTables: tc.mu.lockTables,
		Options:    tc.opts.options,
		Flag:       tc.mu.flag,
	}, nil
}

func (tc *txnOperator) UpdateSnapshot(
	ctx context.Context,
	ts timestamp.Timestamp) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if err := tc.checkStatus(true); err != nil {
		return err
	}

	// ony push model support RC isolation
	if tc.timestampWaiter == nil {
		return nil
	}

	_, err := tc.doCostAction(
		time.Time{},
		UpdateSnapshotEvent,
		func() error {
			var err error
			tc.mu.txn.SnapshotTS, err = tc.timestampWaiter.GetTimestamp(
				ctx,
				ts)
			return err
		},
		true)
	return err
}

func (tc *txnOperator) ApplySnapshot(data []byte) error {
	if !tc.opts.coordinator {
		tc.logger.Fatal("apply snapshot on non-coordinator txn operator")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkStatus(true); err != nil {
		return err
	}

	snapshot := &txn.CNTxnSnapshot{}
	if err := snapshot.Unmarshal(data); err != nil {
		return err
	}

	if !bytes.Equal(snapshot.Txn.ID, tc.mu.txn.ID) {
		tc.logger.Fatal("apply snapshot with invalid txn id")
	}

	// apply locked tables in other cn
	for _, v := range snapshot.LockTables {
		if err := tc.doAddLockTableLocked(v); err != nil {
			return err
		}
	}

	for _, tn := range snapshot.Txn.TNShards {
		has := false
		for _, v := range tc.mu.txn.TNShards {
			if v.ShardID == tn.ShardID {
				has = true
				break
			}
		}

		if !has {
			tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, tn)
		}
	}
	if tc.mu.txn.SnapshotTS.Less(snapshot.Txn.SnapshotTS) {
		tc.mu.txn.SnapshotTS = snapshot.Txn.SnapshotTS
	}
	util.LogTxnUpdated(tc.logger, tc.mu.txn)
	return nil
}

func (tc *txnOperator) Read(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnRead(tc.logger, tc.getTxnMeta(false))

	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Read
	}

	if err := tc.validate(ctx, false); err != nil {
		return nil, err
	}

	requests = tc.maybeInsertCachedWrites(requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnOperator) Write(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnWrite(tc.logger, tc.getTxnMeta(false))
	return tc.doWrite(ctx, requests, false)
}

func (tc *txnOperator) WriteAndCommit(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnWrite(tc.logger, tc.getTxnMeta(false))
	util.LogTxnCommit(tc.logger, tc.getTxnMeta(false))
	return tc.doWrite(ctx, requests, true)
}

func (tc *txnOperator) Commit(ctx context.Context) (err error) {
	if tc.reset.runningSQL.Load() && !tc.markAborted() {
		tc.logger.Fatal("commit on running txn",
			zap.String("txnID", hex.EncodeToString(tc.reset.txnID)))
	}

	tc.reset.commitCounter.addEnter()
	defer tc.reset.commitCounter.addExit()
	txnMeta := tc.getTxnMeta(false)
	util.LogTxnCommit(tc.logger, txnMeta)

	readonly := tc.reset.workspace != nil && tc.reset.workspace.Readonly()
	if !readonly {
		tc.reset.commitSeq = tc.NextSequence()
		tc.reset.commitAt = time.Now()

		tc.triggerEvent(newEvent(CommitEvent, txnMeta, tc.reset.commitSeq, nil))
		defer func() {
			cost := time.Since(tc.reset.commitAt)
			v2.TxnCNCommitDurationHistogram.Observe(cost.Seconds())
			tc.triggerEvent(newCostEvent(CommitEvent, tc.getTxnMeta(false), tc.reset.commitSeq, err, cost))
		}()
	}

	if tc.opts.options.ReadOnly() {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		tc.mu.txn.Status = txn.TxnStatus_Committed
		tc.closeLocked()
		return
	}

	result, e := tc.doWrite(ctx, nil, true)
	if e != nil {
		err = e
		return
	}

	if result != nil {
		result.Release()
	}
	return
}

func (tc *txnOperator) Rollback(ctx context.Context) (err error) {
	if tc.reset.runningSQL.Load() {
		tc.logger.Fatal("rollback on running txn")
	}

	tc.reset.rollbackCounter.addEnter()
	defer tc.reset.rollbackCounter.addExit()
	v2.TxnRollbackCounter.Inc()
	txnMeta := tc.getTxnMeta(false)
	util.LogTxnRollback(tc.logger, txnMeta)

	if tc.reset.workspace != nil && !tc.reset.cannotCleanWorkspace {
		if err = tc.reset.workspace.Rollback(ctx); err != nil {
			tc.logger.Error("rollback workspace failed",
				util.TxnIDField(txnMeta), zap.Error(err))
		}
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.closed {
		return nil
	}

	seq := tc.NextSequence()
	start := time.Now()
	tc.triggerEventLocked(newEvent(RollbackEvent, txnMeta, seq, nil))
	defer func() {
		cost := time.Since(start)
		tc.triggerEventLocked(newCostEvent(RollbackEvent, txnMeta, seq, err, cost))
	}()

	defer func() {
		tc.mu.txn.Status = txn.TxnStatus_Aborted
		tc.closeLocked()
	}()

	if tc.needUnlockLocked() {
		defer tc.unlock(ctx)
	}

	if len(tc.mu.txn.TNShards) == 0 {
		return nil
	}

	result, err := tc.handleError(tc.doSend(ctx, []txn.TxnRequest{{
		Method:          txn.TxnMethod_Rollback,
		RollbackRequest: &txn.TxnRollbackRequest{},
	}}, true))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrTxnClosed) {
			return nil
		}
		return err
	}
	if result != nil {
		result.Release()
	}
	return nil
}

func (tc *txnOperator) AddLockTable(value lock.LockTable) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txn.Mode != txn.TxnMode_Pessimistic {
		panic("lock in optimistic mode")
	}

	// mirror txn can not check status, and the txn's status is on the creation cn of the txn.
	if !tc.mu.txn.Mirror {
		if err := tc.checkStatus(true); err != nil {
			return err
		}
	}

	return tc.doAddLockTableLocked(value)
}

func (tc *txnOperator) HasLockTable(table uint64) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txn.Mode != txn.TxnMode_Pessimistic {
		panic("lock in optimistic mode")
	}

	return tc.hasLockTableLocked(table)
}

func (tc *txnOperator) ResetRetry(retry bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.retry = retry
}

func (tc *txnOperator) IsRetry() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.mu.retry
}

func (tc *txnOperator) doAddLockTableLocked(value lock.LockTable) error {
	for _, l := range tc.mu.lockTables {
		if l.Group == value.Group &&
			l.Table == value.Table {
			if l.Changed(value) {
				return moerr.NewLockTableBindChangedNoCtx()
			}
			return nil
		}
	}
	tc.mu.lockTables = append(tc.mu.lockTables, value)
	return nil
}

func (tc *txnOperator) hasLockTableLocked(table uint64) bool {
	for _, l := range tc.mu.lockTables {
		if l.Table == table {
			return true
		}
	}
	return false
}

func (tc *txnOperator) Debug(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_DEBUG
	}

	if err := tc.validate(ctx, false); err != nil {
		return nil, err
	}

	requests = tc.maybeInsertCachedWrites(requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnOperator) doWrite(ctx context.Context, requests []txn.TxnRequest, commit bool) (*rpc.SendResult, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Write
	}

	if tc.opts.options.ReadOnly() {
		tc.logger.Fatal("can not write on ready only transaction")
	}
	var payload []txn.TxnRequest
	if commit {
		if tc.reset.workspace != nil {
			reqs, err := tc.reset.workspace.Commit(ctx)
			if err != nil {
				return nil, errors.Join(err, tc.Rollback(ctx))
			}
			payload = reqs
		}
		tc.mu.Lock()
		defer func() {
			tc.closeLocked()
			tc.mu.Unlock()
		}()
		if tc.mu.closed {
			tc.reset.commitErr = moerr.NewTxnClosedNoCtx(tc.reset.txnID)
			return nil, tc.reset.commitErr
		}

		if tc.needUnlockLocked() {
			tc.mu.txn.LockTables = tc.mu.lockTables
			defer tc.unlock(ctx)
		}
	}

	if err := tc.validate(ctx, commit); err != nil {
		return nil, err
	}

	var txnReqs []*txn.TxnRequest
	if payload != nil {
		v2.TxnCNCommitCounter.Inc()
		for i := range payload {
			payload[i].Txn = tc.getTxnMeta(true)
			txnReqs = append(txnReqs, &payload[i])
		}
		tc.updateWritePartitions(payload, commit)
	}

	tc.updateWritePartitions(requests, commit)

	// delayWrites enabled, no responses
	if !commit && tc.maybeCacheWrites(requests, commit) {
		return nil, nil
	}

	if commit {
		if len(tc.mu.txn.TNShards) == 0 { // commit no write handled txn
			tc.mu.txn.Status = txn.TxnStatus_Committed
			return nil, nil
		}

		requests = tc.maybeInsertCachedWrites(requests, true)
		requests = append(requests, txn.TxnRequest{
			Method: txn.TxnMethod_Commit,
			Flag:   txn.SkipResponseFlag,
			CommitRequest: &txn.TxnCommitRequest{
				Payload:       txnReqs,
				Disable1PCOpt: tc.opts.options.Is1PCDisabled(),
			}})
	}
	if commit && tc.markAbortedLocked() {
		tc.reset.commitErr = moerr.NewTxnClosedNoCtx(tc.reset.txnID)
		return nil, tc.reset.commitErr
	}

	resp, err := tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, commit)))
	if err != nil && commit {
		tc.reset.commitErr = err
	}
	return resp, err
}

func (tc *txnOperator) updateWritePartitions(requests []txn.TxnRequest, locked bool) {
	if len(requests) == 0 {
		return
	}

	if !locked {
		tc.mu.Lock()
		defer tc.mu.Unlock()
	}

	for _, req := range requests {
		tc.addPartitionLocked(req.CNRequest.Target)
	}
}

func (tc *txnOperator) addPartitionLocked(tn metadata.TNShard) {
	for idx := range tc.mu.txn.TNShards {
		if tc.mu.txn.TNShards[idx].ShardID == tn.ShardID {
			return
		}
	}
	tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, tn)
	util.LogTxnUpdated(tc.logger, tc.mu.txn)
}

func (tc *txnOperator) validate(ctx context.Context, locked bool) error {
	if _, ok := ctx.Deadline(); !ok {
		tc.logger.Fatal("context deadline set")
	}

	return tc.checkStatus(locked)
}

func (tc *txnOperator) checkStatus(locked bool) error {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}

	if tc.mu.closed {
		return moerr.NewTxnClosedNoCtx(tc.reset.txnID)
	}
	return nil
}

func (tc *txnOperator) maybeCacheWrites(requests []txn.TxnRequest, locked bool) bool {
	if tc.opts.options.CacheWriteEnabled() {
		if !locked {
			tc.mu.Lock()
			defer tc.mu.Unlock()
		}

		for idx := range requests {
			requests[idx].Flag |= txn.SkipResponseFlag
			tn := requests[idx].CNRequest.Target.ShardID
			tc.mu.cachedWrites[tn] = append(tc.mu.cachedWrites[tn], requests[idx])
		}
		return true
	}
	return false
}

func (tc *txnOperator) maybeInsertCachedWrites(
	requests []txn.TxnRequest,
	locked bool,
) []txn.TxnRequest {
	if len(requests) == 0 ||
		!tc.opts.options.CacheWriteEnabled() {
		return requests
	}

	if !locked {
		tc.mu.Lock()
		defer tc.mu.Unlock()
	}

	if len(tc.mu.cachedWrites) == 0 {
		return requests
	}

	newRequests := requests
	hasCachedWrites := false
	insertCount := 0
	for idx := range requests {
		tn := requests[idx].CNRequest.Target.ShardID
		if writes, ok := tc.getCachedWritesLocked(tn); ok {
			if !hasCachedWrites {
				// copy all requests into newRequests if cached writes encountered
				newRequests = append([]txn.TxnRequest(nil), requests[:idx]...)
			}
			newRequests = append(newRequests, writes...)
			tc.clearCachedWritesLocked(tn)
			hasCachedWrites = true
			insertCount += len(writes)
		}
		if hasCachedWrites {
			newRequests = append(newRequests, requests[idx])
		}
	}
	return newRequests
}

func (tc *txnOperator) getCachedWritesLocked(tn uint64) ([]txn.TxnRequest, bool) {
	writes, ok := tc.mu.cachedWrites[tn]
	if !ok || len(writes) == 0 {
		return nil, false
	}
	return writes, true
}

func (tc *txnOperator) clearCachedWritesLocked(tn uint64) {
	delete(tc.mu.cachedWrites, tn)
}

func (tc *txnOperator) getTxnMeta(locked bool) txn.TxnMeta {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}
	return tc.mu.txn
}

func (tc *txnOperator) doSend(
	ctx context.Context,
	requests []txn.TxnRequest,
	commit bool,
) (*rpc.SendResult, error) {
	txnMeta := tc.getTxnMeta(commit)
	for idx := range requests {
		requests[idx].Txn = txnMeta
	}

	util.LogTxnSendRequests(tc.logger, requests)
	result, err := tc.sender.Send(ctx, requests)
	if err != nil {
		if commit {
			// TODO: remove this workaround
			// set tc.mu.txn.CommitTS = now+10s
			now, _ := tc.clock.Now()
			now.PhysicalTime += 10000000000
			tc.mu.txn.CommitTS = now
		}
		util.LogTxnSendRequestsFailed(tc.logger, requests, err)
		return nil, err
	}
	util.LogTxnReceivedResponses(tc.logger, result.Responses)

	if len(result.Responses) == 0 {
		return result, nil
	}

	// update commit timestamp
	resp := result.Responses[len(result.Responses)-1]
	if resp.Txn == nil {
		return result, nil
	}
	if !commit {
		tc.mu.Lock()
		defer tc.mu.Unlock()
	}
	tc.mu.txn.CommitTS = resp.Txn.CommitTS
	tc.mu.txn.Status = resp.Txn.Status
	return result, nil
}

func (tc *txnOperator) handleError(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
	if err != nil {
		return nil, err
	}

	for _, resp := range result.Responses {
		if err := tc.handleErrorResponse(resp); err != nil {
			result.Release()
			return nil, err
		}
	}
	return result, nil
}

func (tc *txnOperator) handleErrorResponse(resp txn.TxnResponse) error {
	switch resp.Method {
	case txn.TxnMethod_Read:
		if err := tc.checkResponseTxnStatusForReadWrite(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, readTxnErrors)
	case txn.TxnMethod_Write:
		if err := tc.checkResponseTxnStatusForReadWrite(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, writeTxnErrors)
	case txn.TxnMethod_Commit:
		tc.triggerEventLocked(
			newCostEvent(
				CommitResponseEvent,
				tc.mu.txn,
				tc.reset.commitSeq,
				nil,
				time.Since(tc.reset.commitAt)))

		if err := tc.checkResponseTxnStatusForCommit(resp); err != nil {
			return err
		}

		err := tc.checkTxnError(resp.TxnError, commitTxnErrors)
		if err == nil || !tc.mu.txn.IsPessimistic() {
			return err
		}

		// commit failed, refresh invalid lock tables
		if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
			tc.lockService.ForceRefreshLockTableBinds(
				resp.CommitResponse.InvalidLockTables,
				func(bind lock.LockTable) bool {
					for _, hold := range tc.mu.lockTables {
						if hold.Table == bind.Table && !hold.Changed(bind) {
							return true
						}
					}
					return false
				})
		}

		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) ||
			moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			v, ok := runtime.ServiceRuntime(tc.sid).GetGlobalVariables(runtime.EnableCheckInvalidRCErrors)
			if ok && v.(bool) {
				tc.logger.Fatal("failed",
					zap.Error(err),
					zap.String("txn", hex.EncodeToString(tc.reset.txnID)))
			}
		}
		return err
	case txn.TxnMethod_Rollback:
		if err := tc.checkResponseTxnStatusForRollback(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, rollbackTxnErrors)
	case txn.TxnMethod_DEBUG:
		if resp.TxnError != nil {
			return resp.TxnError.UnwrapError()
		}
		return nil
	default:
		return moerr.NewNotSupportedNoCtxf("unknown txn response method: %s", resp.DebugString())
	}
}

func (tc *txnOperator) checkResponseTxnStatusForReadWrite(resp txn.TxnResponse) error {
	if resp.TxnError != nil {
		return nil
	}

	txnMeta := resp.Txn
	if txnMeta == nil {
		return moerr.NewTxnClosedNoCtx(tc.reset.txnID)
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Active:
		return nil
	case txn.TxnStatus_Aborted, txn.TxnStatus_Aborting,
		txn.TxnStatus_Committed, txn.TxnStatus_Committing:
		return moerr.NewTxnClosedNoCtx(tc.reset.txnID)
	default:
		tc.logger.Fatal("invalid response status for read or write",
			util.TxnField(*txnMeta))
	}
	return nil
}

func (tc *txnOperator) checkTxnError(txnError *txn.TxnError, possibleErrorMap map[uint16]struct{}) error {
	if txnError == nil {
		return nil
	}

	// use txn internal error code to check error
	txnCode := uint16(txnError.TxnErrCode)
	if txnCode == moerr.ErrTNShardNotFound {
		// do we still have the uuid and shard id?
		return moerr.NewTNShardNotFoundNoCtx("", 0xDEADBEAF)
	}

	if _, ok := possibleErrorMap[txnCode]; ok {
		return txnError.UnwrapError()
	}

	panic(moerr.NewInternalErrorNoCtxf("invalid txn error, code %d, msg %s", txnCode, txnError.DebugString()))
}

func (tc *txnOperator) checkResponseTxnStatusForCommit(resp txn.TxnResponse) error {
	if resp.TxnError != nil {
		return nil
	}

	txnMeta := resp.Txn
	if txnMeta == nil {
		return moerr.NewTxnClosedNoCtx(tc.reset.txnID)
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Committed, txn.TxnStatus_Aborted:
		return nil
	default:
		panic(moerr.NewInternalErrorNoCtxf("invalid response status for commit, %v", txnMeta.Status))
	}
}

func (tc *txnOperator) checkResponseTxnStatusForRollback(resp txn.TxnResponse) error {
	if resp.TxnError != nil {
		return nil
	}

	txnMeta := resp.Txn
	if txnMeta == nil {
		return moerr.NewTxnClosedNoCtx(tc.reset.txnID)
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Aborted:
		return nil
	default:
		panic(moerr.NewInternalErrorNoCtxf("invalid response status for rollback %v", txnMeta.Status))
	}
}

func (tc *txnOperator) trimResponses(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
	if err != nil {
		return nil, err
	}

	values := result.Responses[:0]
	for _, resp := range result.Responses {
		if !resp.HasFlag(txn.SkipResponseFlag) {
			values = append(values, resp)
		}
	}
	result.Responses = values
	return result, nil
}

func (tc *txnOperator) unlock(ctx context.Context) {
	if tc.reset.workspace != nil &&
		tc.reset.workspace.Readonly() &&
		len(tc.mu.lockTables) == 0 {
		return
	}

	if !tc.reset.commitAt.IsZero() {
		v2.TxnCNCommitResponseDurationHistogram.Observe(float64(time.Since(tc.reset.commitAt).Seconds()))
	}

	// rc mode need to see the committed value, so wait logtail applied
	if tc.mu.txn.IsRCIsolation() &&
		tc.timestampWaiter != nil {
		cost, err := tc.doCostAction(
			time.Time{},
			CommitWaitApplyEvent,
			func() error {
				_, err := tc.timestampWaiter.GetTimestamp(ctx, tc.mu.txn.CommitTS)
				return err
			},
			true)
		v2.TxnCNCommitWaitLogtailDurationHistogram.Observe(cost.Seconds())

		if err != nil {
			tc.logger.Error("txn wait committed log applied failed in rc mode",
				util.TxnField(tc.mu.txn),
				zap.Error(err))
		}
	}

	_, err := tc.doCostAction(
		time.Time{},
		UnlockEvent,
		func() error {
			return tc.lockService.Unlock(
				ctx,
				tc.mu.txn.ID,
				tc.mu.txn.CommitTS)
		},
		true)
	if err != nil {
		tc.logger.Error("failed to unlock txn",
			util.TxnField(tc.mu.txn),
			zap.Error(err))
	}
}

func (tc *txnOperator) needUnlockLocked() bool {
	if tc.mu.txn.Mode ==
		txn.TxnMode_Optimistic {
		return false
	}
	return tc.lockService != nil
}

func (tc *txnOperator) closeLocked() {
	if !tc.mu.closed {
		tc.mu.closed = true
		if tc.reset.commitErr != nil {
			tc.mu.txn.Status = txn.TxnStatus_Aborted
		}
		tc.triggerEventLocked(
			TxnEvent{
				Event: ClosedEvent,
				Txn:   tc.mu.txn,
				Err:   tc.reset.commitErr,
			})
	}
}

func (tc *txnOperator) AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64 {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.waitLocks == nil {
		tc.mu.waitLocks = make(map[uint64]Lock)
	}

	seq := tc.mu.lockSeq
	tc.mu.lockSeq++

	tc.mu.waitLocks[seq] = Lock{
		TableID: tableID,
		Rows:    rows,
		Options: opt,
	}
	return seq
}

func (tc *txnOperator) RemoveWaitLock(key uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	delete(tc.mu.waitLocks, key)
}

func (tc *txnOperator) GetOverview() TxnOverview {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return TxnOverview{
		CreateAt:  tc.reset.createAt,
		Meta:      tc.mu.txn,
		UserTxn:   tc.opts.options.UserTxn(),
		WaitLocks: tc.getWaitLocksLocked(),
	}
}

func (tc *txnOperator) getWaitLocksLocked() []Lock {
	if tc.mu.waitLocks == nil {
		return nil
	}

	values := make([]Lock, 0, len(tc.mu.waitLocks))
	for _, l := range tc.mu.waitLocks {
		values = append(values, l)
	}
	return values
}

func (tc *txnOperator) LockSkipped(
	tableID uint64,
	mode lock.LockMode) bool {
	if len(tc.opts.options.SkipLockTables) == 0 {
		return false
	}
	for i, id := range tc.opts.options.SkipLockTables {
		if id == tableID &&
			mode == tc.opts.options.SkipLockTableModes[i] {
			return true
		}
	}
	return false
}

func (tc *txnOperator) TxnOptions() txn.TxnOptions {
	return tc.opts.options
}

func (tc *txnOperator) NextSequence() uint64 {
	return tc.reset.sequence.Add(1)
}

func (tc *txnOperator) doCostAction(
	startAt time.Time,
	event EventType,
	action func() error,
	locked bool) (time.Duration, error) {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}

	seq := tc.NextSequence()
	if startAt == (time.Time{}) {
		startAt = time.Now()
	}

	tc.triggerEventLocked(
		newEvent(
			event,
			tc.mu.txn,
			seq,
			nil))

	err := action()
	cost := time.Since(startAt)
	tc.triggerEventLocked(
		newCostEvent(
			event,
			tc.mu.txn,
			seq,
			err,
			time.Since(startAt)))
	return cost, err
}

func (tc *txnOperator) EnterRunSql() {
	tc.reset.runningSQL.Store(true)
	tc.reset.runSqlCounter.addEnter()
}

func (tc *txnOperator) ExitRunSql() {
	tc.reset.runningSQL.Store(false)
	tc.reset.runSqlCounter.addExit()
}

func (tc *txnOperator) inRunSql() bool {
	return tc.reset.runSqlCounter.more()
}

func (tc *txnOperator) inCommit() bool {
	return tc.reset.commitCounter.more()
}

func (tc *txnOperator) inRollback() bool {
	return tc.reset.rollbackCounter.more()
}

func (tc *txnOperator) EnterIncrStmt() {
	tc.reset.incrStmtCounter.addEnter()
}

func (tc *txnOperator) ExitIncrStmt() {
	tc.reset.incrStmtCounter.addExit()
}

func (tc *txnOperator) inIncrStmt() bool {
	return tc.reset.incrStmtCounter.more()
}

func (tc *txnOperator) EnterRollbackStmt() {
	tc.reset.rollbackStmtCounter.addEnter()
}

func (tc *txnOperator) ExitRollbackStmt() {
	tc.reset.rollbackStmtCounter.addExit()
}
func (tc *txnOperator) inRollbackStmt() bool {
	return tc.reset.rollbackStmtCounter.more()
}

func (tc *txnOperator) counter() string {
	return fmt.Sprintf("commit: %s rollback: %s runSql: %s incrStmt: %s rollbackStmt: %s txnMeta: %s footPrints: %s",
		tc.reset.commitCounter.String(),
		tc.reset.rollbackCounter.String(),
		tc.reset.runSqlCounter.String(),
		tc.reset.incrStmtCounter.String(),
		tc.reset.rollbackStmtCounter.String(),
		tc.Txn().DebugString(),
		tc.reset.fprints.String())
}

func (tc *txnOperator) SetFootPrints(id int, enter bool) {
	tc.reset.fprints.add(id, enter)
}

func (tc *txnOperator) addFlag(flags ...uint32) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, flag := range flags {
		tc.mu.flag |= flag
	}
}

func (tc *txnOperator) markAborted() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.markAbortedLocked()
}

func (tc *txnOperator) markAbortedLocked() bool {
	return tc.mu.flag&AbortedFlag != 0
}

func (tc *txnOperator) setWaitActive(v bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.waitActive = v
}
