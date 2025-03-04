// Copyright 2021 - 2024 Matrix Origin
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

package proxy

import (
	"context"
	"time"

	"github.com/petermattis/goid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func (c *clientConn) migrateConnFrom(sqlAddr string) (*query.MigrateConnFromResponse, error) {
	req := c.queryClient.NewRequest(query.CmdMethod_MigrateConnFrom)
	req.MigrateConnFromRequest = &query.MigrateConnFromRequest{
		ConnID: c.connID,
	}
	ctx, cancel := context.WithTimeoutCause(c.ctx, time.Second*3, moerr.CauseMigrateConnFrom)
	defer cancel()
	addr := getQueryAddress(c.moCluster, sqlAddr)
	if addr == "" {
		return nil, moerr.NewInternalError(c.ctx, "cannot get query service address")
	}
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		return nil, moerr.AttachCause(ctx, err)
	}
	r := resp.MigrateConnFromResponse

	c.log.Info("connection migrate from server", zap.String("server address", addr),
		zap.String("tenant", string(c.clientInfo.Tenant)),
		zap.String("username", c.clientInfo.username),
		zap.Uint32("conn ID", c.connID),
		zap.String("DB", r.DB),
		zap.Int("prepare stmt num", len(r.PrepareStmts)),
		zap.Int64("goId", goid.Get()),
	)

	defer c.queryClient.Release(resp)
	return r, nil
}

func (c *clientConn) migrateConnTo(sc ServerConn, info *query.MigrateConnFromResponse) error {
	// Before migrate session info with RPC, we need to execute some
	// SQLs to initialize the session and account in handler.
	// Currently, the session variable transferred is not used anywhere else,
	// and just used here.
	if _, err := sc.ExecStmt(internalStmt{
		cmdType: cmdQuery,
		s:       "/* cloud_nonuser */ set transferred=1;",
	}, nil); err != nil {
		return err
	}

	// First, we re-run the set variables statements.
	for _, stmt := range c.migration.setVarStmts {
		if _, err := sc.ExecStmt(internalStmt{
			cmdType: cmdQuery,
			s:       stmt,
		}, nil); err != nil {
			v2.ProxyConnectCommonFailCounter.Inc()
			return err
		}
	}

	// Then, migrate other info with RPC.
	addr := getQueryAddress(c.moCluster, sc.RawConn().RemoteAddr().String())
	if addr == "" {
		return moerr.NewInternalError(c.ctx, "cannot get query service address")
	}
	c.log.Info("connection migrate to server", zap.String("server address", addr),
		zap.String("tenant", string(c.clientInfo.Tenant)),
		zap.String("username", c.clientInfo.username),
		zap.Uint32("conn ID", c.connID),
		zap.Int64("goId", goid.Get()),
	)
	req := c.queryClient.NewRequest(query.CmdMethod_MigrateConnTo)
	req.MigrateConnToRequest = &query.MigrateConnToRequest{
		ConnID:       c.connID,
		DB:           info.DB,
		PrepareStmts: info.PrepareStmts,
	}
	ctx, cancel := context.WithTimeoutCause(c.ctx, time.Second*3, moerr.CauseMigrateConnTo)
	defer cancel()
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		return moerr.AttachCause(ctx, err)
	}
	c.queryClient.Release(resp)
	return nil
}

func (c *clientConn) migrateConn(prevAddr string, sc ServerConn) error {
	resp, err := c.migrateConnFrom(prevAddr)
	if err != nil {
		return err
	}
	if resp == nil {
		return moerr.NewInternalError(c.ctx, "bad response")
	}
	return c.migrateConnTo(sc, resp)
}
