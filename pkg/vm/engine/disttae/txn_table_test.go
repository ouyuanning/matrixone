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

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
)

func newTxnTableForTest() *txnTable {
	engine := &Engine{
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
	}
	engine.catalog.Store(cache.NewCatalog())
	var tnStore DNStore
	txn := &Transaction{
		engine:   engine,
		tnStores: []DNStore{tnStore},
	}
	rt := runtime.DefaultRuntime()
	s, err := rpc.NewSender(rpc.Config{}, rt)
	if err != nil {
		panic(err)
	}
	c := client.NewTxnClient("", s)
	c.Resume()
	op, _ := c.New(context.Background(), timestamp.Timestamp{})
	op.AddWorkspace(txn)

	db := &txnDatabase{
		op: op,
	}
	table := &txnTable{
		db:         db,
		primaryIdx: 0,
		eng:        engine,
	}
	return table
}

func makeBatchForTest(
	mp *mpool.MPool,
	ints ...int64,
) *batch.Batch {
	bat := batch.New([]string{"a"})
	vec := vector.NewVec(types.T_int64.ToType())
	for _, n := range ints {
		vector.AppendFixed(vec, n, false, mp)
	}
	bat.SetVector(0, vec)
	bat.SetRowCount(len(ints))
	return bat
}

func TestTxnTable_Reset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("nil workspace", func(t *testing.T) {
		tbl := newTxnTableForTest()
		newOp, closeFn := client.NewTestTxnOperator(ctx)
		defer closeFn()
		assert.Error(t, tbl.Reset(newOp))
	})

	t.Run("ok", func(t *testing.T) {
		tbl := newTxnTableForTest()
		newOp, closeFn := client.NewTestTxnOperator(ctx)
		defer closeFn()
		newProc := &process.Process{}
		newOp.AddWorkspace(&Transaction{
			proc: newProc,
		})
		assert.NoError(t, tbl.Reset(newOp))
		assert.Equal(t, newOp, tbl.db.op)
		assert.Equal(t, newProc, tbl.proc.Load())
	})

	t.Run("delegate", func(t *testing.T) {
		op, closeFn := client.NewTestTxnOperator(ctx)
		defer closeFn()
		proc := &process.Process{
			Base: &process.BaseProcess{},
		}
		op.AddWorkspace(&Transaction{
			proc: proc,
		})
		orig, err := newTxnTable(ctx, &txnDatabase{
			op: op,
		}, cache.TableItem{})
		assert.NoError(t, err)
		rt := runtime.DefaultRuntime()
		runtime.SetupServiceBasedRuntime("s1", rt)
		mc := clusterservice.NewMOCluster("s1", nil, time.Second,
			clusterservice.WithDisableRefresh())
		rt.SetGlobalVariables(runtime.ClusterService, mc)
		st := shardservice.NewShardStorage("", rt.Clock(), nil, nil, nil, nil)
		sv := shardservice.NewService(shardservice.Config{
			ServiceID: "s1",
		}, st)
		tbl, err := MockTableDelegate(orig, sv)
		assert.NoError(t, err)
		assert.NotNil(t, tbl)

		newOp, closeFn1 := client.NewTestTxnOperator(ctx)
		defer closeFn1()
		newProc := &process.Process{
			Base: &process.BaseProcess{},
		}
		newOp.AddWorkspace(&Transaction{
			proc: newProc,
		})
		assert.NoError(t, tbl.Reset(newOp))

		tbl.(*txnTableDelegate).combined.is = true
		tbl.(*txnTableDelegate).combined.tbl = &combinedTxnTable{
			primary: newTxnTableForTest(),
		}
		assert.NoError(t, tbl.Reset(newOp))
	})
}

// func TestPrimaryKeyCheck(t *testing.T) {
// 	ctx := context.Background()
// 	mp := mpool.MustNewZero()

// 	getRowIDsBatch := func(table *txnTable) *batch.Batch {
// 		bat := batch.New(false, []string{catalog.Row_ID})
// 		vec := vector.NewVec(types.T_Rowid.ToType())
// 		iter := table.localState.NewRowsIter(
// 			types.TimestampToTS(table.nextLocalTS()),
// 			nil,
// 			false,
// 		)
// 		l := 0
// 		for iter.Next() {
// 			entry := iter.Entry()
// 			vector.AppendFixed(vec, entry.RowID, false, mp)
// 			l++
// 		}
// 		iter.Close()
// 		bat.SetVector(0, vec)
// 		bat.SetZs(l, mp)
// 		return bat
// 	}

// 	table := newTxnTableForTest(mp)

// 	// insert
// 	err := table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 1),
// 	)
// 	assert.Nil(t, err)

// 	// // insert duplicated
// 	// we check duplicated in pipeline runing now
// 	// err = table.Write(
// 	// 	ctx,
// 	// 	makeBatchForTest(mp, 1),
// 	// )
// 	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

// 	// insert no duplicated
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 2, 3),
// 	)
// 	assert.Nil(t, err)

// 	// duplicated in same batch
// 	// we check duplicated in pipeline runing now
// 	// err = table.Write(
// 	// 	ctx,
// 	// 	makeBatchForTest(mp, 4, 4),
// 	// )
// 	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

// 	table = newTxnTableForTest(mp)

// 	// insert, delete then insert
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 1),
// 	)
// 	assert.Nil(t, err)
// 	err = table.Delete(
// 		ctx,
// 		getRowIDsBatch(table),
// 		catalog.Row_ID,
// 	)
// 	assert.Nil(t, err)
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 5),
// 	)
// 	assert.Nil(t, err)

// }

func BenchmarkTxnTableInsert(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	table := newTxnTableForTest()
	for i, max := int64(0), int64(b.N); i < max; i++ {
		err := table.Write(
			ctx,
			makeBatchForTest(mp, i),
		)
		assert.Nil(b, err)
	}
}
