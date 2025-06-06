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

package ioutil

import (
	"context"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// ConstructTombstoneWriter hiddenSelection: true for add hidden columns `commitTs` and `abort`
// object file created by `CN` with `hiddenSelection` is false
func ConstructTombstoneWriter(
	hiddenSelection objectio.HiddenColumnSelection,
	fs fileservice.FileService,
) *BlockWriter {
	seqnums := objectio.GetTombstoneSeqnums(hiddenSelection)
	sortkeyPos := objectio.TombstonePrimaryKeyIdx
	sortkeyIsPK := true
	isTombstone := true
	return ConstructWriter(
		0,
		seqnums,
		sortkeyPos,
		sortkeyIsPK,
		isTombstone,
		fs,
	)
}

func ConstructWriterWithSegmentID(
	segmentID *objectio.Segmentid,
	num uint16,
	ver uint32,
	seqnums []uint16,
	sortkeyPos int,
	sortkeyIsPK bool,
	isTombstone bool,
	fs fileservice.FileService,
	arena *objectio.WriteArena,
) *BlockWriter {
	name := objectio.BuildObjectName(segmentID, num)
	return constructWriterWithName(name,
		ver, seqnums, sortkeyPos, sortkeyIsPK, isTombstone, fs, arena)
}

func ConstructWriter(
	ver uint32,
	seqnums []uint16,
	sortkeyPos int,
	sortkeyIsPK bool,
	isTombstone bool,
	fs fileservice.FileService,
) *BlockWriter {
	noid := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&noid)
	return constructWriterWithName(name,
		ver, seqnums, sortkeyPos, sortkeyIsPK, isTombstone, fs, nil)
}

func constructWriterWithName(
	name objectio.ObjectName,
	ver uint32,
	seqnums []uint16,
	sortkeyPos int,
	sortkeyIsPK bool,
	isTombstone bool,
	fs fileservice.FileService,
	arena *objectio.WriteArena,
) *BlockWriter {
	writer, err := NewBlockWriterNew(
		fs,
		name,
		ver,
		seqnums,
		isTombstone,
	)
	if err != nil {
		panic(err) // it is impossible
	}
	// has sortkey
	if sortkeyPos >= 0 {
		if sortkeyIsPK {
			if isTombstone {
				writer.SetPrimaryKeyWithType(
					uint16(objectio.TombstonePrimaryKeyIdx),
					index.HBF,
					index.ObjectPrefixFn,
					index.BlockPrefixFn,
				)
			} else {
				writer.SetPrimaryKey(uint16(sortkeyPos))
			}
		} else { // cluster by
			writer.SetSortKey(uint16(sortkeyPos))
		}
	}
	return writer
}

type BlockWriter struct {
	writer         *objectio.ObjectWriter
	objMetaBuilder *ObjectColumnMetasBuilder
	isSetPK        bool
	pk             uint16
	pkType         uint8
	sortKeyIdx     uint16
	nameStr        string
	name           objectio.ObjectName
	prefix         []index.PrefixFn

	isTombstone bool
}

func NewBlockWriter(fs fileservice.FileService, name string) (*BlockWriter, error) {
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterETL, name, fs)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		writer:     writer,
		isSetPK:    false,
		sortKeyIdx: math.MaxUint16,
		nameStr:    name,
	}, nil
}

// seqnums is the column's seqnums of the batch written by `WriteBatch`. `WriteBatchWithoutIndex` will ignore the seqnums
func NewBlockWriterNew(
	fs fileservice.FileService,
	name objectio.ObjectName,
	schemaVer uint32,
	seqnums []uint16,
	isTombstone bool,
) (*BlockWriter, error) {
	return NewBlockWriterWithArena(fs, name, schemaVer, seqnums, isTombstone, nil)
}

func NewBlockWriterWithArena(
	fs fileservice.FileService,
	name objectio.ObjectName,
	schemaVer uint32,
	seqnums []uint16,
	isTombstone bool,
	arena *objectio.WriteArena,
) (*BlockWriter, error) {
	writer, err := objectio.NewObjectWriter(name, fs, schemaVer, seqnums, arena)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		writer:      writer,
		isSetPK:     false,
		sortKeyIdx:  math.MaxUint16,
		nameStr:     name.String(),
		name:        name,
		isTombstone: isTombstone,
	}, nil
}

func (w *BlockWriter) SetTombstone() {
	w.isTombstone = true
}

func (w *BlockWriter) SetPrimaryKey(idx uint16) {
	w.isSetPK = true
	w.pk = idx
	w.sortKeyIdx = idx
	w.pkType = index.BF
}

func (w *BlockWriter) SetPrimaryKeyWithType(idx uint16, pkType uint8, prefix ...index.PrefixFn) {
	w.isSetPK = true
	w.pk = idx
	w.sortKeyIdx = idx
	w.pkType = pkType
	w.prefix = prefix
}

func (w *BlockWriter) SetSortKey(idx uint16) {
	w.sortKeyIdx = idx
}

func (w *BlockWriter) SetAppendable() {
	w.writer.SetAppendable()
}

func (w *BlockWriter) GetObjectStats(opts ...objectio.ObjectStatsOptions) objectio.ObjectStats {
	return w.writer.GetObjectStats(opts...)
}

func (w *BlockWriter) GetWrittenOriginalSize() uint32 {
	return w.writer.GetOrignalSize()
}

// WriteBatch write a batch whose schema is decribed by seqnum in NewBlockWriterNew, write batch to memroy cache, not S3
func (w *BlockWriter) WriteBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	if w.objMetaBuilder == nil {
		w.objMetaBuilder = NewObjectColumnMetasBuilder(len(batch.Vecs))
	}
	seqnums := w.writer.GetSeqnums()
	if w.sortKeyIdx != math.MaxUint16 {
		w.writer.SetSortKeySeqnum(seqnums[w.sortKeyIdx])
	}
	for i, vec := range batch.Vecs {
		isPK := false
		if i == 0 {
			w.objMetaBuilder.AddRowCnt(vec.Length())
		}

		// PXU TODO: change this logic
		if !w.isTombstone {
			// only skip SchemaData type
			if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
				continue
			}
		}

		if w.isSetPK && w.pk == uint16(i) {
			isPK = true
		}
		columnData := containers.ToTNVector(vec, common.DefaultAllocator)
		// update null count and distinct value
		w.objMetaBuilder.InspectVector(i, columnData, isPK)

		// Build ZM
		zm := index.NewZM(vec.GetType().Oid, vec.GetType().Scale)
		if err = index.BatchUpdateZM(zm, columnData.GetDownstreamVector()); err != nil {
			return nil, err
		}

		index.SetZMSum(zm, columnData.GetDownstreamVector())
		// Update column meta zonemap
		w.writer.UpdateBlockZM(objectio.SchemaData, int(block.GetID()), seqnums[i], zm)
		// update object zonemap
		w.objMetaBuilder.UpdateZm(i, zm)

		if !w.isSetPK || w.pk != uint16(i) {
			continue
		}
		w.objMetaBuilder.AddPKData(columnData)
		var bf index.StaticFilter
		if w.pkType == index.BF {
			bf, err = index.NewBloomFilter(columnData)
		} else if w.pkType == index.PBF {
			if len(w.prefix) < 1 {
				return nil, index.ErrPrefix
			}
			prefix := w.prefix[0]
			bf, err = index.NewPrefixBloomFilter(columnData, prefix.Id, prefix.Fn)
		} else if w.pkType == index.HBF {
			if len(w.prefix) < 2 {
				return nil, index.ErrPrefix
			}
			prefixL1 := w.prefix[0]
			prefixL2 := w.prefix[1]
			bf, err = index.NewHybridBloomFilter(columnData, prefixL1.Id, prefixL1.Fn, prefixL2.Id, prefixL2.Fn)
		}
		if err != nil {
			return nil, err
		}
		buf, err := bf.Marshal()
		if err != nil {
			return nil, err
		}

		if err = w.writer.WriteBF(int(block.GetID()), seqnums[i], buf, w.pkType); err != nil {
			return nil, err
		}
	}

	return block, nil
}

func (w *BlockWriter) WriteSubBatch(batch *batch.Batch, dataType objectio.DataMetaType) (objectio.BlockObject, int, error) {
	return w.writer.WriteSubBlock(batch, dataType)
}

func (w *BlockWriter) Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error) {
	if w.objMetaBuilder != nil {
		if w.isSetPK {
			w.objMetaBuilder.SetPKNdv(w.pk, w.objMetaBuilder.GetTotalRow())
		}
		cnt, meta := w.objMetaBuilder.Build()
		w.writer.WriteObjectMeta(ctx, cnt, meta)
	}
	blocks, err := w.writer.WriteEnd(ctx)
	if len(blocks) == 0 {
		logutil.Debug("[WriteEnd]", common.OperationField(w.nameStr),
			common.OperandField("[Size=0]"), common.OperandField(w.writer.GetSeqnums()))
		return blocks, objectio.Extent{}, err
	}

	logutil.Debug("[WriteEnd]",
		common.OperationField(w.String(blocks)),
		common.OperandField(w.writer.GetSeqnums()),
		common.OperandField(w.writer.GetMaxSeqnum()))
	return blocks, blocks[0].BlockHeader().MetaLocation(), err
}
func (w *BlockWriter) Stats() objectio.ObjectStats {
	return w.writer.GetDataStats()
}
func (w *BlockWriter) GetName() objectio.ObjectName {
	return w.name
}

func (w *BlockWriter) String(
	blocks []objectio.BlockObject,
) string {
	size, _ := objectio.SumSizeOfBlocks(blocks)
	return fmt.Sprintf("name: %s, block count: %d, size: %d",
		w.nameStr,
		len(blocks),
		size,
	)
}
