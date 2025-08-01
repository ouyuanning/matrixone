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

package tnservice

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

var (
	supportTxnStorageBackends = map[StorageType]struct{}{
		StorageMEMKV: {},
		StorageMEM:   {},
		StorageTAE:   {},
	}
)

func (s *store) createTxnStorage(
	ctx context.Context,
	shard metadata.TNShard,
	txnServer rpc.TxnServer,
) (storage.TxnStorage, error) {

	factory := s.createLogServiceClientFactroy(shard)
	closeLogClientFn := func(logClient logservice.Client) {
		if err := logClient.Close(); err != nil {
			s.rt.Logger().Error("close log client failed",
				zap.Error(err))
		}
	}

	switch s.cfg.Txn.Storage.Backend {
	case StorageMEM:
		logClient, err := factory()
		if err != nil {
			return nil, err
		}
		ts, err := s.newMemTxnStorage(shard, logClient, s.hakeeperClient)
		if err != nil {
			closeLogClientFn(logClient)
			return nil, err
		}
		return ts, nil

	case StorageMEMKV:
		logClient, err := factory()
		if err != nil {
			return nil, err
		}
		return s.newMemKVStorage(shard, logClient)

	case StorageTAE:
		ts, err := s.newTAEStorage(ctx, shard, factory, txnServer)
		if err != nil {
			return nil, err
		}
		return ts, nil
	default:
		return nil, moerr.NewInternalErrorf(ctx, "not implment for %s", s.cfg.Txn.Storage.Backend)
	}
}

func (s *store) createLogServiceClient(shard metadata.TNShard) (logservice.Client, error) {
	if s.options.logServiceClientFactory != nil {
		return s.options.logServiceClientFactory(shard)
	}
	return s.newLogServiceClient(shard)
}

func (s *store) createLogServiceClientFactroy(shard metadata.TNShard) logservice.ClientFactory {
	return func() (logservice.Client, error) {
		return s.createLogServiceClient(shard)
	}
}

func (s *store) newLogServiceClient(shard metadata.TNShard) (logservice.Client, error) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), s.cfg.LogService.ConnectTimeout.Duration, moerr.CauseNewLogServiceClient)
	defer cancel()
	client, err := logservice.NewClient(ctx, s.cfg.UUID, logservice.ClientConfig{
		ReadOnly:         false,
		LogShardID:       shard.LogShardID,
		TNReplicaID:      shard.ReplicaID,
		ServiceAddresses: s.cfg.HAKeeper.ClientConfig.ServiceAddresses,
		DiscoveryAddress: s.cfg.HAKeeper.ClientConfig.DiscoveryAddress,
		MaxMessageSize:   int(s.cfg.RPC.MaxMessageSize),
	})
	if err != nil {
		return nil, moerr.AttachCause(ctx, err)
	}
	return client, nil
}

func (s *store) newMemTxnStorage(
	shard metadata.TNShard,
	logClient logservice.Client,
	hakeeper logservice.TNHAKeeperClient,
) (storage.TxnStorage, error) {
	// should it be no fixed or a certain size?
	mp, err := mpool.NewMPool("mem_txn_storge", 0, mpool.NoFixed)
	if err != nil {
		return nil, err
	}
	return memorystorage.NewMemoryStorage(
		s.cfg.UUID,
		mp,
		s.rt.Clock(),
		memoryengine.NewHakeeperIDGenerator(hakeeper),
	)
}

func (s *store) newMemKVStorage(shard metadata.TNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	return mem.NewKVTxnStorage(0, logClient, s.rt.Clock()), nil
}

func (s *store) newTAEStorage(
	ctx context.Context,
	shard metadata.TNShard,
	factory logservice.ClientFactory,
	txnServer rpc.TxnServer,
) (storage.TxnStorage, error) {

	// use s3 as main fs
	fs, err := fileservice.Get[fileservice.FileService](s.fileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}

	// local fs
	localFs, err2 := fileservice.Get[fileservice.FileService](s.fileService, defines.LocalFileServiceName)
	if err2 != nil {
		return nil, err2
	}

	// tmp fs
	tmpFs, err := fileservice.Get[fileservice.FileService](s.fileService, defines.TmpFileServiceName)
	if err != nil {
		return nil, err
	}

	ckpcfg := &options.CheckpointCfg{
		MinCount:               s.cfg.Ckp.MinCount,
		ScanInterval:           s.cfg.Ckp.ScanInterval.Duration,
		FlushInterval:          s.cfg.Ckp.FlushInterval.Duration,
		MetadataCheckInterval:  s.cfg.Ckp.MetadataCheckInterval.Duration,
		IncrementalInterval:    s.cfg.Ckp.IncrementalInterval.Duration,
		GlobalVersionInterval:  s.cfg.Ckp.GlobalVersionInterval.Duration,
		GlobalMinCount:         s.cfg.Ckp.GlobalMinCount,
		ReservedWALEntryCount:  s.cfg.Ckp.ReservedWALEntryCount,
		OverallFlushMemControl: s.cfg.Ckp.OverallFlushMemControl,
	}

	gcCfg := &options.GCCfg{
		GCTTL:          s.cfg.GCCfg.GCTTL.Duration,
		GCInMemoryTTL:  s.cfg.GCCfg.GCInMemoryTTL.Duration,
		ScanGCInterval: s.cfg.GCCfg.ScanGCInterval.Duration,
		DisableGC:      s.cfg.GCCfg.DisableGC,
		CheckGC:        s.cfg.GCCfg.CheckGC,
		CacheSize:      s.cfg.GCCfg.CacheSize,
		GCMergeCount:   s.cfg.GCCfg.GCMergeCount,
		GCestimateRows: s.cfg.GCCfg.GCestimateRows,
		GCProbility:    s.cfg.GCCfg.GCProbility,
	}

	mergeCfg := &options.MergeConfig{
		CNMergeMemControlHint: uint64(s.cfg.Merge.CNMergeMemHint),
		CNTakeOverAll:         s.cfg.Merge.CNTakeOverAll,
		CNTakeOverExceed:      uint64(s.cfg.Merge.CNTakeOverExceed),
		CNStandaloneTake:      s.cfg.Merge.CNStandaloneTake,
		DisableZMBasedMerge:   s.cfg.Merge.DisableZMBasedMerge,
	}

	logtailServerAddr := s.logtailServiceListenAddr()
	logtailServerCfg := &options.LogtailServerCfg{
		RpcMaxMessageSize:      int64(s.cfg.LogtailServer.RpcMaxMessageSize),
		RpcEnableChecksum:      s.cfg.LogtailServer.RpcEnableChecksum,
		RPCStreamPoisonTime:    s.cfg.LogtailServer.LogtailRPCStreamPoisonTime.Duration,
		LogtailCollectInterval: s.cfg.LogtailServer.LogtailCollectInterval.Duration,
		ResponseSendTimeout:    s.cfg.LogtailServer.LogtailResponseSendTimeout.Duration,
		PullWorkerPoolSize:     int64(s.cfg.LogtailServer.PullWorkerPoolSize),
	}

	// the previous values
	//max2LogServiceMsgSizeLimit := s.cfg.RPC.MaxMessageSize
	// unlimited, divided by 2 to avoid overflow
	max2LogServiceMsgSizeLimit := uint64(math.MaxUint64 / 2)

	opt := &options.Options{
		Clock:                s.rt.Clock(),
		Fs:                   fs,
		LocalFs:              localFs,
		TmpFs:                tmpFs.(*fileservice.TmpFileService),
		WalClientFactory:     logservicedriver.LogServiceClientFactory(factory),
		Shard:                shard,
		CheckpointCfg:        ckpcfg,
		GCCfg:                gcCfg,
		MergeCfg:             mergeCfg,
		IncrementalDedup:     s.cfg.Txn.IncrementalDedup == "true",
		IsStandalone:         s.cfg.InStandalone,
		Ctx:                  ctx,
		MaxMessageSize:       max2LogServiceMsgSizeLimit,
		TaskServiceGetter:    s.GetTaskService,
		SID:                  s.cfg.UUID,
		EnableApplyTableData: s.cfg.Txn.DebugMode,
	}

	return taestorage.NewTAEStorage(
		ctx,
		s.cfg.Txn.Storage.dataDir,
		opt,
		shard,
		s.rt,
		logtailServerAddr,
		logtailServerCfg,
		txnServer,
	)
}
