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

package logservice

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	defaultBackendReadTimeout = time.Second * 8
)

type basicHAKeeperClient interface {
	// Close closes the hakeeper client.
	Close() error
	// AllocateID allocate a globally unique ID
	AllocateID(ctx context.Context) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error)
	// GetClusterDetails queries the HAKeeper and return CN and TN nodes that are
	// known to the HAKeeper.
	GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error)
	// GetClusterState queries the cluster state
	GetClusterState(ctx context.Context) (pb.CheckerState, error)
	// CheckLogServiceHealth checks if the log-service is healthy or not.
	CheckLogServiceHealth(ctx context.Context) error
}

// ClusterHAKeeperClient used to get cluster detail
type ClusterHAKeeperClient interface {
	basicHAKeeperClient
}

// CNHAKeeperClient is the HAKeeper client used by a CN store.
type CNHAKeeperClient interface {
	basicHAKeeperClient
	BRHAKeeperClient
	// SendCNHeartbeat sends the specified heartbeat message to the HAKeeper.
	SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error)
	// UpdateNonVotingReplicaNum updates the non-voting-replica-num which is stores in HAKeeper.
	UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error
	// UpdateNonVotingLocality updates the non-voting-locality which is stores in HAKeeper.
	UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error
}

// TNHAKeeperClient is the HAKeeper client used by a TN store.
type TNHAKeeperClient interface {
	basicHAKeeperClient
	// SendTNHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// TN store.
	SendTNHeartbeat(ctx context.Context, hb pb.TNStoreHeartbeat) (pb.CommandBatch, error)
}

// LogHAKeeperClient is the HAKeeper client used by a Log store.
type LogHAKeeperClient interface {
	basicHAKeeperClient
	// SendLogHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// Log store.
	SendLogHeartbeat(ctx context.Context, hb pb.LogStoreHeartbeat) (pb.CommandBatch, error)
}

// ProxyHAKeeperClient is the HAKeeper client used by proxy service.
type ProxyHAKeeperClient interface {
	basicHAKeeperClient
	// GetCNState gets CN state from HAKeeper.
	GetCNState(ctx context.Context) (pb.CNState, error)
	// UpdateCNLabel updates the labels of CN.
	UpdateCNLabel(ctx context.Context, label pb.CNStoreLabel) error
	// UpdateCNWorkState updates the work state of CN.
	UpdateCNWorkState(ctx context.Context, state pb.CNWorkState) error
	// PatchCNStore updates the work state and labels of CN.
	PatchCNStore(ctx context.Context, stateLabel pb.CNStateLabel) error
	// DeleteCNStore deletes a CN store from HAKeeper.
	DeleteCNStore(ctx context.Context, cnStore pb.DeleteCNStore) error
	// SendProxyHeartbeat sends the heartbeat of proxy to HAKeeper.
	SendProxyHeartbeat(ctx context.Context, hb pb.ProxyHeartbeat) (pb.CommandBatch, error)
}

// BRHAKeeperClient is the HAKeeper client for backup and restore.
type BRHAKeeperClient interface {
	GetBackupData(ctx context.Context) ([]byte, error)
}

// TODO: HAKeeper discovery to be implemented

var _ CNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ TNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ LogHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ ProxyHAKeeperClient = (*managedHAKeeperClient)(nil)

func NewClusterHAKeeperClient(
	ctx context.Context, sid string, cfg HAKeeperClientConfig,
) (ClusterHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewCNHAKeeperClient creates a HAKeeper client to be used by a CN node.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewCNHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (CNHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewTNHAKeeperClient creates a HAKeeper client to be used by a TN node.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewTNHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (TNHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewLogHAKeeperClient creates a HAKeeper client to be used by a Log Service node.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewLogHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (LogHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewLogHAKeeperClientWithRetry creates a HAKeeper client with retry.
func NewLogHAKeeperClientWithRetry(
	ctx context.Context, sid string, cfg HAKeeperClientConfig,
) ClusterHAKeeperClient {
	var c ClusterHAKeeperClient
	createFn := func() error {
		ctx, cancel := context.WithTimeoutCause(
			ctx, time.Second*5, moerr.CauseNewLogHAKeeperClientWithRetry,
		)
		defer cancel()
		client, err := NewClusterHAKeeperClient(ctx, sid, cfg)
		if err != nil {
			err = moerr.AttachCause(ctx, err)
			logutil.Errorf("failed to create HAKeeper client: %v", err)
			return err
		}
		c = client
		return nil
	}
	timer := time.NewTimer(time.Minute * 2)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			panic("failed to create HAKeeper client")

		default:
			if err := createFn(); err != nil {
				time.Sleep(time.Second * 3)
				continue
			}
			return c
		}
	}
}

// NewProxyHAKeeperClient creates a HAKeeper client to be used by a proxy service.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewProxyHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (ProxyHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

func newManagedHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (*managedHAKeeperClient, error) {
	c, err := newHAKeeperClient(ctx, sid, cfg)
	if err != nil {
		return nil, err
	}

	mc := &managedHAKeeperClient{
		cfg:            cfg,
		sid:            sid,
		backendOptions: GetBackendOptions(ctx),
		clientOptions:  GetClientOptions(ctx),
	}
	mc.mu.client = c
	mc.mu.allocIDByKey = make(map[string]*allocID)
	return mc, nil
}

// allocID contains nextID and lastID.
type allocID struct {
	nextID uint64
	lastID uint64
}

type managedHAKeeperClient struct {
	sid string
	cfg HAKeeperClientConfig

	// Method `prepareClient` may update moprc.Client.
	// So we need to keep options for morpc.Client.
	backendOptions []morpc.BackendOption
	clientOptions  []morpc.ClientOption

	mu struct {
		sync.RWMutex
		// allocIDByKey is used to alloc IDs by different key.
		allocIDByKey map[string]*allocID
		// sharedAllocID is used to alloc global IDs.
		sharedAllocID allocID

		client *hakeeperClient
	}
}

func (c *managedHAKeeperClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.client == nil {
		return nil
	}
	return c.mu.client.close()
}

// CheckLogServiceHealth implements the ClusterHAKeeperClient interface.
func (c *managedHAKeeperClient) CheckLogServiceHealth(ctx context.Context) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		details, err := c.getClient().getClusterDetails(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				c.resetClient()
				continue
			}
			return err
		}
		if len(details.TNStores) == 0 {
			// there are no tn stores yet.
			return nil
		}
		var err1 error
		for _, tnStore := range details.TNStores {
			for _, shard := range tnStore.Shards {
				err1 = firstError(err1, c.getClient().checkLogServiceHealth(
					ctx,
					pb.CheckHealth{
						ShardID: shard.ShardID,
					},
				))
			}
		}
		return err1
	}
}

func (c *managedHAKeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.ClusterDetails{}, err
		}
		cd, err := c.getClient().getClusterDetails(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cd, err
	}
}

func (c *managedHAKeeperClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CheckerState{}, err
		}
		s, err := c.getClient().getClusterState(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return s, err
	}
}

func (c *managedHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	if c.mu.sharedAllocID.nextID != c.mu.sharedAllocID.lastID {
		v := c.mu.sharedAllocID.nextID
		c.mu.sharedAllocID.nextID++
		c.mu.Unlock()
		if v == 0 {
			logutil.Error("id should not be 0",
				zap.Uint64("nextID", c.mu.sharedAllocID.nextID),
				zap.Uint64("lastID", c.mu.sharedAllocID.lastID))
		}
		return v, nil
	}

	defer c.mu.Unlock()

	batchSize := c.cfg.AllocateIDBatch
	for {
		if err := c.prepareClientLocked(ctx); err != nil {
			return 0, err
		}
		firstID, err := c.mu.client.sendCNAllocateID(ctx, "", batchSize)

		if err != nil {
			c.resetClientLocked()
			if c.isRetryableError(err) {
				continue
			}
			logutil.Error("failed to allocate id",
				zap.Error(err),
				zap.Uint64("batch", c.cfg.AllocateIDBatch),
				zap.Uint64("nextID", c.mu.sharedAllocID.nextID),
				zap.Uint64("lastID", c.mu.sharedAllocID.lastID),
			)
			return 0, err
		}

		c.mu.sharedAllocID.nextID = firstID + 1
		c.mu.sharedAllocID.lastID = firstID + batchSize - 1
		return firstID, err
	}
}

// AllocateIDByKey implements the basicHAKeeperClient interface.
func (c *managedHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	if key == "" {
		return c.AllocateID(ctx)
	}
	return c.AllocateIDByKeyWithBatch(ctx, key, c.cfg.AllocateIDBatch)
}

func (c *managedHAKeeperClient) AllocateIDByKeyWithBatch(
	ctx context.Context,
	key string,
	batchSize uint64) (uint64, error) {
	// empty key is used in shared allocated IDs.
	if len(key) == 0 {
		return 0, moerr.NewInternalError(ctx, "key should not be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	allocIDs, ok := c.mu.allocIDByKey[key]
	if !ok {
		allocIDs = &allocID{nextID: 0, lastID: 0}
		c.mu.allocIDByKey[key] = allocIDs
	}

	if allocIDs.nextID != allocIDs.lastID {
		v := allocIDs.nextID
		allocIDs.nextID++
		return v, nil
	}

	for {
		if err := c.prepareClientLocked(ctx); err != nil {
			return 0, err
		}
		firstID, err := c.mu.client.sendCNAllocateID(ctx, key, batchSize)
		if err != nil {
			c.resetClientLocked()
			if c.isRetryableError(err) {
				continue
			}
			return 0, err
		}

		allocIDs.nextID = firstID + 1
		allocIDs.lastID = firstID + batchSize - 1
		return firstID, err
	}
}

func (c *managedHAKeeperClient) SendCNHeartbeat(ctx context.Context,
	hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		result, err := c.getClient().sendCNHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return result, err
	}
}

func (c *managedHAKeeperClient) SendTNHeartbeat(ctx context.Context,
	hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		cb, err := c.getClient().sendTNHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cb, err
	}
}

func (c *managedHAKeeperClient) SendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		cb, err := c.getClient().sendLogHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cb, err
	}
}

// GetCNState implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) GetCNState(ctx context.Context) (pb.CNState, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CNState{}, err
		}
		s, err := c.getClient().getCNState(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return s, err
	}
}

// UpdateCNLabel implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateCNLabel(
	ctx context.Context, label pb.CNStoreLabel,
) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.getClient().updateCNLabel(ctx, label)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

// UpdateCNWorkState implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateCNWorkState(
	ctx context.Context, state pb.CNWorkState,
) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.getClient().updateCNWorkState(ctx, state)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

// PatchCNStore implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) PatchCNStore(
	ctx context.Context, stateLabel pb.CNStateLabel,
) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.getClient().patchCNStore(ctx, stateLabel)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

// DeleteCNStore implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) DeleteCNStore(
	ctx context.Context, cnStore pb.DeleteCNStore,
) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.getClient().deleteCNStore(ctx, cnStore)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

// SendProxyHeartbeat implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) SendProxyHeartbeat(
	ctx context.Context, hb pb.ProxyHeartbeat,
) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		cb, err := c.getClient().sendProxyHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cb, err
	}
}

// GetBackupData implements the BRHAKeeperClient interface.
func (c *managedHAKeeperClient) GetBackupData(ctx context.Context) ([]byte, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return nil, err
		}
		s, err := c.getClient().getBackupData(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return s, err
	}
}

// UpdateNonVotingReplicaNum implements the CNHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateNonVotingReplicaNum(
	ctx context.Context, num uint64,
) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.getClient().updateNonVotingReplicaNum(ctx, num)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

// UpdateNonVotingLocality implements the CNHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateNonVotingLocality(
	ctx context.Context, locality pb.Locality,
) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.getClient().updateNonVotingLocality(ctx, locality)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

func (c *managedHAKeeperClient) isRetryableError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper)
}

func (c *managedHAKeeperClient) resetClient() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetClientLocked()
}

func (c *managedHAKeeperClient) prepareClient(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepareClientLocked(ctx)
}

func (c *managedHAKeeperClient) resetClientLocked() {
	if c.mu.client != nil {
		cc := c.mu.client
		c.mu.client = nil
		if err := cc.close(); err != nil {
			logutil.Error("failed to close client", zap.Error(err))
		}
	}
}

func (c *managedHAKeeperClient) prepareClientLocked(ctx context.Context) error {
	if c.mu.client != nil {
		return nil
	}

	// we must use the recoreded options for morpc.Client
	ctx = SetBackendOptions(ctx, c.backendOptions...)
	ctx = SetClientOptions(ctx, c.clientOptions...)

	cc, err := newHAKeeperClient(ctx, c.sid, c.cfg)
	if err != nil {
		return err
	}
	c.mu.client = cc
	return nil
}

type hakeeperClient struct {
	cfg      HAKeeperClientConfig
	client   morpc.RPCClient
	addr     string
	pool     *sync.Pool
	respPool *sync.Pool
}

func newHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (*hakeeperClient, error) {
	var err error
	var c *hakeeperClient
	// If the discovery address is configured, we used it first.
	if len(cfg.DiscoveryAddress) > 0 {
		c, err = connectByReverseProxy(ctx, sid, cfg.DiscoveryAddress, cfg)
		if c != nil && err == nil {
			return c, nil
		}
	} else if len(cfg.ServiceAddresses) > 0 {
		c, err = connectToHAKeeper(ctx, sid, cfg.ServiceAddresses, cfg)
		if c != nil && err == nil {
			return c, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return nil, moerr.NewNoHAKeeper(ctx)
}

func connectByReverseProxy(
	ctx context.Context,
	sid string,
	discoveryAddress string,
	cfg HAKeeperClientConfig,
) (*hakeeperClient, error) {
	si, ok, err := GetShardInfo(sid, discoveryAddress, hakeeper.DefaultHAKeeperShardID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	addresses := make([]string, 0)
	leaderAddress, ok := si.Replicas[si.ReplicaID]
	if ok {
		addresses = append(addresses, leaderAddress)
	}
	for replicaID, address := range si.Replicas {
		if replicaID != si.ReplicaID {
			addresses = append(addresses, address)
		}
	}
	return connectToHAKeeper(ctx, sid, addresses, cfg)
}

func connectToHAKeeper(
	ctx context.Context,
	sid string,
	targets []string,
	cfg HAKeeperClientConfig,
) (*hakeeperClient, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	c := &hakeeperClient{
		cfg:      cfg,
		pool:     pool,
		respPool: respPool,
	}
	var e error
	addresses := append([]string{}, targets...)
	rand.Shuffle(len(addresses), func(i, j int) {
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})
	for _, addr := range addresses {
		cc, err := getRPCClient(
			ctx,
			sid,
			addr,
			c.respPool,
			defaultMaxMessageSize,
			cfg.EnableCompress,
			defaultBackendReadTimeout,
			"connectToHAKeeper",
		)
		if err != nil {
			e = err
			continue
		}
		c.addr = addr
		c.client = cc
		isHAKeeper, err := c.checkIsHAKeeper(ctx)
		logutil.Info(fmt.Sprintf("isHAKeeper: %t, err: %v", isHAKeeper, err))
		if err == nil && isHAKeeper {
			return c, nil
		} else if err != nil {
			e = err
		}
		if err := cc.Close(); err != nil {
			logutil.Error("failed to close the client", zap.Error(err))
		}
	}
	if e == nil {
		// didn't encounter any error
		return nil, moerr.NewNoHAKeeper(ctx)
	}
	return nil, e
}

func (c *hakeeperClient) close() error {
	if c == nil {
		panic("!!!")
	}

	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *hakeeperClient) getClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_DETAILS,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.ClusterDetails{}, err
	}
	return *resp.ClusterDetails, nil
}

func (c *hakeeperClient) getClusterState(ctx context.Context) (pb.CheckerState, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_STATE,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.CheckerState{}, err
	}
	return *resp.CheckerState, nil
}

func (c *hakeeperClient) checkLogServiceHealth(ctx context.Context, checkHealth pb.CheckHealth) error {
	req := pb.Request{
		Method:      pb.CHECK_HEALTH,
		CheckHealth: &checkHealth,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	if resp.ErrorCode != 0 {
		return moerr.NewInternalError(ctx, resp.ErrorMessage)
	}
	return nil
}

func (c *hakeeperClient) sendCNHeartbeat(
	ctx context.Context, hb pb.CNStoreHeartbeat,
) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.CN_HEARTBEAT,
		CNHeartbeat: &hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) sendCNAllocateID(
	ctx context.Context, key string, batch uint64,
) (uint64, error) {
	req := pb.Request{
		Method:       pb.CN_ALLOCATE_ID,
		CNAllocateID: &pb.CNAllocateID{Key: key, Batch: batch},
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.AllocateID.FirstID, nil
}

func (c *hakeeperClient) sendTNHeartbeat(ctx context.Context,
	hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.TN_HEARTBEAT,
		TNHeartbeat: &hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) sendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:       pb.LOG_HEARTBEAT,
		LogHeartbeat: &hb,
	}
	cb, err := c.sendHeartbeat(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	for _, cmd := range cb.Commands {
		logutil.Info("hakeeper client received cmd", zap.String("cmd", cmd.LogString()))
	}
	return cb, nil
}

func (c *hakeeperClient) sendHeartbeat(ctx context.Context,
	req pb.Request) (pb.CommandBatch, error) {
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	if resp.CommandBatch == nil {
		return pb.CommandBatch{}, nil
	}
	return *resp.CommandBatch, nil
}

func (c *hakeeperClient) getCNState(ctx context.Context) (pb.CNState, error) {
	s, err := c.getClusterState(ctx)
	if err != nil {
		return pb.CNState{}, err
	}
	return s.CNState, nil
}

func (c *hakeeperClient) updateCNLabel(ctx context.Context, label pb.CNStoreLabel) error {
	req := pb.Request{
		Method:       pb.UPDATE_CN_LABEL,
		CNStoreLabel: &label,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) updateCNWorkState(ctx context.Context, state pb.CNWorkState) error {
	req := pb.Request{
		Method:      pb.UPDATE_CN_WORK_STATE,
		CNWorkState: &state,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) patchCNStore(ctx context.Context, stateLabel pb.CNStateLabel) error {
	req := pb.Request{
		Method:       pb.PATCH_CN_STORE,
		CNStateLabel: &stateLabel,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) deleteCNStore(ctx context.Context, cnStore pb.DeleteCNStore) error {
	req := pb.Request{
		Method:        pb.DELETE_CN_STORE,
		DeleteCNStore: &cnStore,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) sendProxyHeartbeat(
	ctx context.Context, hb pb.ProxyHeartbeat,
) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:         pb.PROXY_HEARTBEAT,
		ProxyHeartbeat: &hb,
	}
	cb, err := c.sendHeartbeat(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	return cb, nil
}

func (c *hakeeperClient) updateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	req := pb.Request{
		Method:              pb.UPDATE_NON_VOTING_REPLICA_NUM,
		NonVotingReplicaNum: num,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) updateNonVotingLocality(
	ctx context.Context, locality pb.Locality,
) error {
	req := pb.Request{
		Method:            pb.UPDATE_NON_VOTING_LOCALITY,
		NonVotingLocality: &locality,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) checkIsHAKeeper(ctx context.Context) (bool, error) {
	req := pb.Request{
		Method: pb.CHECK_HAKEEPER,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return false, err
	}
	return resp.IsHAKeeper, nil
}

func (c *hakeeperClient) request(ctx context.Context, req pb.Request) (pb.Response, error) {
	if c == nil {
		return pb.Response{}, moerr.NewNoHAKeeper(ctx)
	}
	ctx, span := trace.Debug(ctx, "hakeeperClient.request")
	defer span.End()
	r := c.pool.Get().(*RPCRequest)
	r.Request = req
	future, err := c.client.Send(ctx, c.addr, r)
	if err != nil {
		return pb.Response{}, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return pb.Response{}, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	err = toError(ctx, response.Response)
	if err != nil {
		return pb.Response{}, err
	}
	return resp, nil
}

func (c *managedHAKeeperClient) getClient() *hakeeperClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.client
}

func (c *hakeeperClient) getBackupData(ctx context.Context) ([]byte, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_STATE,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return nil, err
	}
	p := pb.BackupData{
		NextID:      resp.CheckerState.NextId,
		NextIDByKey: resp.CheckerState.NextIDByKey,
	}
	bs, err := p.Marshal()
	if err != nil {
		return nil, err
	}
	return bs, nil
}
