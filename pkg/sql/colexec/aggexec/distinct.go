// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type distinctHash struct {
	maps []*hashmap.StrHashMap
	itrs []hashmap.Iterator

	// optimized for bulk and batch insertions.
	bs  []bool
	bs1 []bool
}

func newDistinctHash() distinctHash {
	return distinctHash{
		maps: nil,
		itrs: nil,
	}
}

func (d *distinctHash) grows(more int) error {
	oldLen, newLen := len(d.maps), len(d.maps)+more
	d.maps = append(d.maps, make([]*hashmap.StrHashMap, more)...)
	d.itrs = append(d.itrs, make([]hashmap.Iterator, more)...)

	var err error
	for i := oldLen; i < newLen; i++ {
		if d.maps[i], err = hashmap.NewStrHashMap(true); err != nil {
			return err
		}
		d.itrs[i] = d.maps[i].NewIterator()
	}
	return nil
}

// fill inserts the row into the hash map.
// return true if this is a new value.
func (d *distinctHash) fill(group int, vs []*vector.Vector, row int) (bool, error) {
	return d.itrs[group].DetectDup(vs, row)
}

func (d *distinctHash) bulkFill(group int, vs []*vector.Vector) ([]bool, error) {
	rowCount := vs[0].Length()

	if cap(d.bs) < rowCount {
		d.bs = make([]bool, rowCount)
	}
	if cap(d.bs1) < hashmap.UnitLimit {
		d.bs1 = make([]bool, hashmap.UnitLimit)
	}
	d.bs = d.bs[:rowCount]
	d.bs1 = d.bs1[:hashmap.UnitLimit]

	iterator := d.itrs[group]

	for i := 0; i < rowCount; i += hashmap.UnitLimit {
		n := rowCount - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		for j := 0; j < n; j++ {
			d.bs1[j] = false
		}

		oldLen := d.maps[group].GroupCount()
		indexOffset := oldLen + 1

		values, _, err := iterator.Insert(i, n, vs)
		if err != nil {
			return nil, err
		}

		dd := d.bs[i:]
		for k, v := range values {
			if v > oldLen && !d.bs1[v-indexOffset] {
				d.bs1[v-indexOffset] = true
				dd[k] = true
			} else {
				dd[k] = false
			}
		}
	}
	return d.bs, nil
}

func (d *distinctHash) batchFill(vs []*vector.Vector, offset int, groups []uint64) ([]bool, error) {
	rowCount := len(groups)

	if cap(d.bs) < rowCount {
		d.bs = make([]bool, rowCount)
	}
	d.bs = d.bs[:0]

	for _, group := range groups {
		if group != GroupNotMatched {
			ok, err := d.fill(int(group-1), vs, offset)
			if err != nil {
				return nil, err
			}
			d.bs = append(d.bs, ok)
		} else {
			d.bs = append(d.bs, false)
		}
		offset++
	}

	return d.bs, nil
}

// merge was the method to merge two groups of distinct agg.
// but distinct agg should be run in only one node and without any parallel.
// because the distinct agg need to store all the source data to make sure the result is correct if we use parallel.
// there is one simple example that:
//
//	select count(distinct a) from t;
//	and `a` is a column with 1, 2, 3, 3, 5
//	if we use parallel, and the data is split into two parts: [1, 2, 3] and [3, 5].
//	once we do the merge, we will get the result 5 from (3 + 2), but the correct result should be 4 from (3 + 1).
//	we need to loop the [3, 5] to do a new data fill to make sure the result is correct, but not do 3 + 2.
//
// this action to store all the source data is very expensive.
//
// I add this check to make sure the distinct agg is not used in parallel.
func (d *distinctHash) merge(next *distinctHash) error {
	if len(d.maps) > 0 || len(next.maps) > 0 {
		return moerr.NewInternalErrorNoCtx("distinct agg should be run in only one node and without any parallel")
	}
	return nil
}

func (d *distinctHash) free() {
	for _, m := range d.maps {
		if m != nil {
			m.Free()
		}
	}
}

func (d *distinctHash) Size() int64 {
	var size int64
	for _, m := range d.maps {
		if m != nil {
			size += m.Size()
		}
	}
	// 8 is the size of a pointer.
	size += int64(cap(d.maps)) * 8
	// 16 is the size of an interface.
	size += int64(cap(d.itrs)) * 16
	size += int64(cap(d.bs))
	size += int64(cap(d.bs1))
	return size
}

func (d *distinctHash) marshal() ([]byte, error) {
	if len(d.maps) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	n := uint64(len(d.maps))
	if _, err := buf.Write(types.EncodeUint64(&n)); err != nil {
		return nil, err
	}

	for _, m := range d.maps {
		data, err := m.MarshalBinary()
		if err != nil {
			return nil, err
		}
		l := uint64(len(data))
		if _, err := buf.Write(types.EncodeUint64(&l)); err != nil {
			return nil, err
		}
		if _, err := buf.Write(data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (d *distinctHash) unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(data)

	var n uint64
	if _, err := buf.Read(types.EncodeUint64(&n)); err != nil {
		return err
	}

	d.maps = make([]*hashmap.StrHashMap, n)
	d.itrs = make([]hashmap.Iterator, n)
	for i := uint64(0); i < n; i++ {
		var l uint64
		if _, err := buf.Read(types.EncodeUint64(&l)); err != nil {
			return err
		}
		mapData := make([]byte, l)
		if _, err := io.ReadFull(buf, mapData); err != nil {
			return err
		}
		d.maps[i] = &hashmap.StrHashMap{}
		if err := d.maps[i].UnmarshalBinary(mapData, hashtable.DefaultAllocator()); err != nil {
			return err
		}
		d.itrs[i] = d.maps[i].NewIterator()
	}
	return nil
}
