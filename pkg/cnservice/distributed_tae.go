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

package cnservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func (s *service) initDistributedTAE(
	ctx context.Context,
	pu *config.ParameterUnit,
) error {

	// txn client
	client, err := s.getTxnClient()
	if err != nil {
		return err
	}
	pu.TxnClient = client

	// hakeeper
	hakeeper, err := s.getHAKeeperClient()
	if err != nil {
		return err
	}

	// use s3 as main fs
	fs, err := fileservice.Get[fileservice.FileService](s.fileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	colexec.NewServer(hakeeper)

	// start I/O pipeline
	ioutil.Start(s.cfg.UUID)

	internalExecutorFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(s.cfg.UUID)
	}

	// engine
	distributeTaeMp, err := mpool.NewMPool("distributed_tae", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	s.distributeTaeMp = distributeTaeMp
	s.storeEngine = disttae.New(
		ctx,
		s.cfg.UUID,
		distributeTaeMp,
		fs,
		client,
		hakeeper,
		s.gossipNode.StatsKeyRouter(),
		s.cfg.LogtailUpdateWorkerFactor,

		disttae.WithCNTransferTxnLifespanThreshold(
			s.cfg.Engine.CNTransferTxnLifespanThreshold),
		disttae.WithMoTableStatsConf(s.cfg.Engine.Stats),
		disttae.WithSQLExecFunc(internalExecutorFactory),
		disttae.WithMoServerStateChecker(func() bool {
			return frontend.MoServerIsStarted(s.cfg.UUID)
		}),
	)
	pu.StorageEngine = s.storeEngine

	// internal sql executor.
	// InitLoTailPushModel presupposes that the internal sql executor has been initialized.
	internalExecutorMp, _ := mpool.NewMPool("internal_executor", 0, mpool.NoFixed)
	s.initInternalSQlExecutor(internalExecutorMp)

	// set up log tail client to subscribe table and receive table log.
	cnEngine := pu.StorageEngine.(*disttae.Engine)
	err = cnEngine.InitLogTailPushModel(ctx, s.timestampWaiter)
	if err != nil {
		return err
	}

	ss, ok := runtime.ServiceRuntime(s.cfg.UUID).GetGlobalVariables(runtime.StatusServer)
	if ok {
		statusServer := ss.(*status.Server)
		statusServer.SetTxnClient(s.cfg.UUID, client)
		statusServer.SetLogTailClient(s.cfg.UUID, cnEngine.PushClient())
	}

	s.initProcessCodecService()
	s.initPartitionService()
	s.initShardService()
	return nil
}
