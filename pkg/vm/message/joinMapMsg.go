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

package message

import (
	"bytes"
	"context"
	"strconv"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var _ Message = new(JoinMapMsg)

const selsDivideLength = 256
const selsPreAlloc = 4

type JoinSels struct {
	sels [][][]int32
}

func (js *JoinSels) InitSel(len int) {
	js.sels = make([][][]int32, 0, len/selsDivideLength+1)
}

func (js *JoinSels) Free() {
	js.sels = nil
}

func (js *JoinSels) InsertSel(k, v int32) {
	i := k / selsDivideLength
	j := k % selsDivideLength
	if len(js.sels) <= int(i) {
		s := make([][]int32, selsDivideLength)
		js.sels = append(js.sels, s)
		var internalArray [selsDivideLength * selsPreAlloc]int32
		for p := 0; p < selsDivideLength; p++ {
			js.sels[i][p] = internalArray[p*selsPreAlloc : p*selsPreAlloc : (p+1)*selsPreAlloc]
		}
	}
	js.sels[i][j] = append(js.sels[i][j], v)
}

func (js *JoinSels) GetSels(k int32) []int32 {
	i := k / selsDivideLength
	j := k % selsDivideLength
	return js.sels[i][j]
}

// JoinMap is used for join
type JoinMap struct {
	runtimeFilter_In bool
	valid            bool
	rowCnt           int64 // for debug purpose
	refCnt           int64
	mpool            *mpool.MPool
	shm              *hashmap.StrHashMap
	ihm              *hashmap.IntHashMap
	multiSels        JoinSels
	delRows          *bitmap.Bitmap
	batches          []*batch.Batch
}

func NewJoinMap(sels JoinSels, ihm *hashmap.IntHashMap, shm *hashmap.StrHashMap, delRows *bitmap.Bitmap, batches []*batch.Batch, m *mpool.MPool) *JoinMap {
	return &JoinMap{
		valid:     true,
		mpool:     m,
		shm:       shm,
		ihm:       ihm,
		multiSels: sels,
		delRows:   delRows,
		batches:   batches,
	}
}

func (jm *JoinMap) GetBatches() []*batch.Batch {
	if jm == nil {
		return nil
	}
	return jm.batches
}

func (jm *JoinMap) SetRowCount(cnt int64) {
	jm.rowCnt = cnt
}

func (jm *JoinMap) GetRefCount() int64 {
	if jm == nil {
		return 0
	}
	return atomic.LoadInt64(&jm.refCnt)
}

func (jm *JoinMap) GetRowCount() int64 {
	if jm == nil {
		return 0
	}
	return jm.rowCnt
}

func (jm *JoinMap) GetGroupCount() uint64 {
	if jm.ihm != nil {
		return jm.ihm.GroupCount()
	}
	return jm.shm.GroupCount()
}

func (jm *JoinMap) SetPushedRuntimeFilterIn(b bool) {
	jm.runtimeFilter_In = b
}

func (jm *JoinMap) PushedRuntimeFilterIn() bool {
	return jm.runtimeFilter_In
}

func (jm *JoinMap) HashOnUnique() bool {
	return jm.multiSels.sels == nil
}

func (jm *JoinMap) GetSels(k uint64) []int32 {
	return jm.multiSels.GetSels(int32(k))
}

//func (jm *JoinMap) GetIgnoreRows() *bitmap.Bitmap {
//	return jm.ignoreRows
//}

//func (jm *JoinMap) SetIgnoreRows(ignoreRows *bitmap.Bitmap) {
//	jm.ignoreRows = ignoreRows
//}

func (jm *JoinMap) NewIterator() hashmap.Iterator {
	if jm.shm != nil {
		return jm.shm.NewIterator()
	} else {
		return jm.ihm.NewIterator()
	}
}

func (jm *JoinMap) IncRef(cnt int32) {
	atomic.AddInt64(&jm.refCnt, int64(cnt))
}

func (jm *JoinMap) IsValid() bool {
	return jm.valid
}

func (jm *JoinMap) IsDeleted(row uint64) bool {
	return jm.delRows != nil && jm.delRows.Contains(uint64(row))
}

func (jm *JoinMap) FreeMemory() {
	jm.multiSels.Free()
	if jm.ihm != nil {
		jm.ihm.Free()
		jm.ihm = nil
	} else if jm.shm != nil {
		jm.shm.Free()
		jm.shm = nil
	}
	for i := range jm.batches {
		jm.batches[i].Clean(jm.mpool)
	}
	jm.batches = nil
	jm.valid = false
}

func (jm *JoinMap) Free() {
	if atomic.AddInt64(&jm.refCnt, -1) != 0 {
		return
	}
	jm.FreeMemory()
}

func (jm *JoinMap) Size() int64 {
	// TODO: add the size of the other JoinMap parts
	if jm.ihm == nil && jm.shm == nil {
		return 0
	}
	if jm.ihm != nil {
		return jm.ihm.Size()
	} else {
		return jm.shm.Size()
	}
}

func (jm *JoinMap) PreAlloc(n uint64) error {
	if jm.ihm != nil {
		return jm.ihm.PreAlloc(n)
	}
	return jm.shm.PreAlloc(n)
}

type JoinMapMsg struct {
	JoinMapPtr *JoinMap
	IsShuffle  bool
	ShuffleIdx int32
	Tag        int32
}

func (t JoinMapMsg) Serialize() []byte {
	panic("top value message only broadcasts on current CN, don't need to serialize")
}

func (t JoinMapMsg) Deserialize([]byte) Message {
	panic("top value message only broadcasts on current CN, don't need to deserialize")
}

func (t JoinMapMsg) NeedBlock() bool {
	return true
}

func (t JoinMapMsg) Destroy() {
	if t.JoinMapPtr != nil {
		t.JoinMapPtr.FreeMemory()
	}
}

func (t JoinMapMsg) GetMsgTag() int32 {
	return t.Tag
}

func (t JoinMapMsg) DebugString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	buf.WriteString("joinmap message, tag:" + strconv.Itoa(int(t.Tag)) + "\n")
	if t.IsShuffle {
		buf.WriteString("shuffle index " + strconv.Itoa(int(t.ShuffleIdx)) + "\n")
	}
	if t.JoinMapPtr != nil {
		buf.WriteString("joinmap rowcnt " + strconv.Itoa(int(t.JoinMapPtr.rowCnt)) + "\n")
		buf.WriteString("joinmap refcnt " + strconv.Itoa(int(t.JoinMapPtr.GetRefCount())) + "\n")
	} else {
		buf.WriteString("joinmapPtr is nil \n")
	}
	return buf.String()
}

func (t JoinMapMsg) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}

func ReceiveJoinMap(tag int32, isShuffle bool, shuffleIdx int32, mb *MessageBoard, ctx context.Context) (*JoinMap, error) {
	msgReceiver := NewMessageReceiver([]int32{tag}, AddrBroadCastOnCurrentCN(), mb)
	for {
		msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, ctx)
		if err != nil {
			return nil, err
		}
		if ctxDone {
			return nil, nil
		}
		for i := range msgs {
			msg, ok := msgs[i].(JoinMapMsg)
			if !ok {
				panic("expect join map message, receive unknown message!")
			}
			if isShuffle || msg.IsShuffle {
				if shuffleIdx != msg.ShuffleIdx {
					continue
				}
			}
			jm := msg.JoinMapPtr
			if jm == nil {
				return nil, nil
			}
			if !jm.IsValid() {
				panic("join receive a joinmap which has been freed!")
			}
			return jm, nil
		}
	}
}

func FinalizeJoinMapMessage(mb *MessageBoard, tag int32, isShuffle bool, shuffleIdx int32, sendMapSucceed bool) {
	if !sendMapSucceed {
		SendMessage(JoinMapMsg{JoinMapPtr: nil, IsShuffle: isShuffle, ShuffleIdx: shuffleIdx, Tag: tag}, mb)
	}
}
