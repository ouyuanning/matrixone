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

package client

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeakCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	ts := newTestTxnSender()

	cc := make(chan struct{})
	c := NewTxnClient(
		"",
		ts,
		WithEnableLeakCheck(
			time.Millisecond*200,
			func([]ActiveTxn) {
				close(cc)
			}))
	c.Resume()
	_, err := c.New(ctx, newTestTimestamp(0))
	assert.Nil(t, err)
	<-cc
	require.NoError(t, c.Close())
}

func TestLeakCheckWithNoLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	ts := newTestTxnSender()

	n := 0
	c := NewTxnClient(
		"",
		ts,
		WithEnableLeakCheck(
			time.Millisecond*200,
			func([]ActiveTxn) {
				n++
			}))
	c.Resume()
	op, err := c.New(ctx, newTestTimestamp(0))
	require.NoError(t, err)
	require.NoError(t, op.Rollback(ctx))
	time.Sleep(time.Millisecond * 200 * 3)
	assert.Equal(t, 0, n)
	lc := c.(*txnClient).leakChecker
	lc.Lock()
	assert.Equal(t, 0, len(lc.actives))
	lc.Unlock()
	require.NoError(t, c.Close())
}

func BenchmarkLeakCheckerTxnClosed(b *testing.B) {
	// Create a leak checker instance
	lc := newLeakCheck(10*time.Second, func([]ActiveTxn) {})

	// Prepare 10,000 active transactions
	for i := 0; i < 10000; i++ {
		txnID := make([]byte, 8)
		txnID[0] = byte(i)
		lc.txnOpened(nil, txnID, txn.TxnOptions{})
	}

	// Create a transaction ID that doesn't exist in active transactions
	nonExistentTxnID := make([]byte, 8)
	nonExistentTxnID[0] = 0xFF

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.txnClosed(nonExistentTxnID)
	}
}
