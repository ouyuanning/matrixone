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

package insert

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "insert"

func (insert *Insert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": insert")
}

func (insert *Insert) OpType() vm.OpType {
	return vm.Insert
}

func (insert *Insert) Prepare(proc *process.Process) error {
	if insert.OpAnalyzer == nil {
		insert.OpAnalyzer = process.NewAnalyzer(insert.GetIdx(), insert.IsFirst, insert.IsLast, "insert")
	} else {
		insert.OpAnalyzer.Reset()
	}

	insert.ctr.state = vm.Build
	if insert.ToWriteS3 {
		fs, err := colexec.GetSharedFSFromProc(proc)
		if err != nil {
			return err
		}

		// If the target is not partition table, you only need to operate the main table
		s3Writer := colexec.NewCNS3DataWriter(
			proc.Mp(), fs, insert.InsertCtx.TableDef, insert.isMemoryTable())

		insert.ctr.s3Writer = s3Writer

		if insert.ctr.buf == nil {
			insert.initBufForS3()
		}
	} else {
		ref := insert.InsertCtx.Ref
		eng := insert.InsertCtx.Engine

		if insert.ctr.source == nil {
			rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref)
			if err != nil {
				return err
			}
			insert.ctr.source = rel
		} else {
			err := insert.ctr.source.Reset(proc.GetTxnOperator())
			if err != nil {
				return err
			}
		}

		if insert.ctr.buf == nil {
			insert.ctr.buf = batch.NewWithSize(len(insert.InsertCtx.Attrs))
			insert.ctr.buf.SetAttributes(insert.InsertCtx.Attrs)
		}
	}
	insert.ctr.affectedRows = 0
	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func (insert *Insert) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := insert.OpAnalyzer

	t := time.Now()
	defer func() {
		analyzer.AddInsertTime(t)
	}()

	if insert.ToWriteS3 {
		return insert.insert_s3(proc, analyzer)
	}
	return insert.insert_table(proc, analyzer)
}

func (insert *Insert) insert_s3(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if insert.ctr.state == vm.Build {
		for {
			input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
			if err != nil {
				return input, err
			}

			if input.Batch == nil {
				insert.ctr.state = vm.Eval
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}

			if insert.InsertCtx.AddAffectedRows {
				affectedRows := uint64(input.Batch.RowCount())
				atomic.AddUint64(&insert.ctr.affectedRows, affectedRows)
			}

			// write to s3.
			input.Batch.Attrs = append(input.Batch.Attrs[:0], insert.InsertCtx.Attrs...)
			err = insert.ctr.s3Writer.Write(proc.Ctx, input.Batch)
			if err != nil {
				insert.ctr.state = vm.End
				return vm.CancelResult, err
			}
		}
	}

	result := vm.NewCallResult()
	result.Batch = insert.ctr.buf
	if insert.ctr.state == vm.Eval {
		writer := insert.ctr.s3Writer
		// handle the last Batch that batchSize less than DefaultBlockMaxRows
		// for more info, refer to the comments about reSizeBatch.
		//
		// data returned to the result would be raw data if it is a memory table.
		err := flushTailBatch(proc, writer, &result, analyzer, insert.isMemoryTable())
		if err != nil {
			insert.ctr.state = vm.End
			return result, err
		}
		insert.ctr.state = vm.End
		return result, nil
	}

	if insert.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

func (insert *Insert) insert_table(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	if !insert.delegated {
		input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
		if err != nil {
			return input, err
		}

		if input.Batch == nil || input.Batch.IsEmpty() {
			return input, nil
		}

		insert.input = input
	}

	input := insert.input
	affectedRows := uint64(input.Batch.RowCount())
	insert.ctr.buf.CleanOnlyData()
	for i := range insert.ctr.buf.Attrs {
		if insert.ctr.buf.Vecs[i] == nil {
			insert.ctr.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
		}
		if err := insert.ctr.buf.Vecs[i].UnionBatch(input.Batch.Vecs[i], 0, input.Batch.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			return input, err
		}
	}
	insert.ctr.buf.SetRowCount(input.Batch.RowCount())

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	// insert into table, insertBat will be deeply copied into txn's workspace.
	err := insert.ctr.source.Write(newCtx, insert.ctr.buf)
	if err != nil {
		return input, err
	}
	analyzer.AddWrittenRows(int64(insert.ctr.buf.RowCount()))
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	if insert.InsertCtx.AddAffectedRows {
		atomic.AddUint64(&insert.ctr.affectedRows, affectedRows)
	}
	// `insertBat` does not include partition expression columns
	return input, nil
}

func flushTailBatch(
	proc *process.Process,
	writer *colexec.CNS3Writer,
	result *vm.CallResult,
	analyzer process.Analyzer,
	isMemoryTable bool,
) error {

	var (
		err error
		bat *batch.Batch
	)

	if !isMemoryTable {
		crs := analyzer.GetOpCounterSet()

		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		if _, err = writer.Sync(newCtx); err != nil {
			return err
		}

		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)

		if bat, err = writer.FillBlockInfoBat(); err != nil {
			return err
		}

		result.Batch, err = result.Batch.Append(proc.Ctx, proc.GetMPool(), bat)
		if err != nil {
			return err
		}

		writer.ResetBlockInfoBat()

		return nil
	}

	return writer.OutputRawData(proc, result.Batch)
}
