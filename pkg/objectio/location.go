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

package objectio

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	ExtentOff   = ObjectNameLen
	ExtentLen   = ExtentSize
	RowsOff     = ExtentOff + ExtentLen
	RowsLen     = 4
	BlockIDOff  = RowsOff + RowsLen
	BlockIDLen  = 2
	LocationLen = BlockIDOff + BlockIDLen
)

const (
	FileNumOff         = SegmentIdSize
	FileNumLen         = 2
	NameStringOff      = FileNumOff + FileNumLen
	NameStringLen      = 42 //uuid[36]+_[1]+filename[5]
	ObjectNameLen      = NameStringOff + NameStringLen
	ObjectNameShortLen = NameStringOff
)

/*
Location is a fixed-length unmodifiable byte array.
Layout:  ObjectName | Extent | Rows(uint32) | ID(uint16)
*/
type Location []byte

func BuildLocation(name ObjectName, extent Extent, rows uint32, id uint16) Location {
	var location [LocationLen]byte
	BuildLocationTo(name, extent, rows, id, location[:])
	return location[:]
}

func BuildLocationTo(
	name ObjectName,
	extent Extent,
	rows uint32,
	id uint16,
	toLoc []byte,
) {
	copy((toLoc)[:ObjectNameLen], name)
	copy((toLoc)[ExtentOff:ExtentOff+ExtentSize], extent)
	copy((toLoc)[RowsOff:RowsOff+RowsLen], types.EncodeUint32(&rows))
	copy((toLoc)[BlockIDOff:BlockIDOff+BlockIDLen], types.EncodeUint16(&id))
}

func NewRandomLocation(id uint16, rows uint32) Location {
	objID := NewObjectid()
	objName := BuildObjectNameWithObjectID(&objID)
	extent := NewRandomExtent()
	return BuildLocation(objName, extent, rows, id)
}

func (l Location) Clone() Location {
	m := make([]byte, 0, len(l))
	return append(m, l...)
}

func (l Location) Name() ObjectName {
	return ObjectName(l[:ObjectNameLen])
}

func (l Location) ObjectId() ObjectId {
	return *(*ObjectId)(unsafe.Pointer(&l[0]))
}

func (l Location) ShortName() *ObjectNameShort {
	return (*ObjectNameShort)(unsafe.Pointer(&l[0]))
}

func (l Location) Extent() Extent {
	return Extent(l[ExtentOff : ExtentOff+ExtentLen])
}

func (l Location) Rows() uint32 {
	return types.DecodeUint32(l[RowsOff : RowsOff+RowsLen])
}
func (l Location) SetRows(rows uint32) {
	copy(l[RowsOff:RowsOff+RowsLen], types.EncodeUint32(&rows))
}

func (l Location) ID() uint16 {
	return types.DecodeUint16(l[BlockIDOff : BlockIDOff+BlockIDLen])
}

func (l Location) SetID(id uint16) {
	copy(l[BlockIDOff:BlockIDOff+BlockIDLen], types.EncodeUint16(&id))
}

func (l Location) IsEmpty() bool {
	return len(l) < LocationLen || types.DecodeInt64(l[:ObjectNameLen]) == 0
}

func (l Location) String() string {
	if len(l) == 0 {
		return ""
	}
	if len(l) != LocationLen {
		return string(l)
	}
	return fmt.Sprintf("%v_%v_%d_%d", l.Name().String(), l.Extent(), l.Rows(), l.ID())
}

func DecodeLocation(buf []byte) *Location {
	location := Location(buf)
	return &location
}

func EncodeLocation(location Location) []byte {
	return location[:]
}

type LocationSlice []byte

func (s *LocationSlice) Get(i int) *Location {
	return DecodeLocation((*s)[i*LocationLen : i*LocationLen+LocationLen])
}

func (s *LocationSlice) GetBytes(i int) []byte {
	return (*s)[i*LocationLen : (i+1)*LocationLen]
}

func (s *LocationSlice) Set(i int, location *Location) {
	copy((*s)[i*LocationLen:], EncodeLocation(*location))
}

func (s *LocationSlice) Len() int {
	return len(*s) / LocationLen
}

func (s *LocationSlice) Size() int {
	return len(*s)
}

func (s *LocationSlice) Slice(i, j int) []byte {
	return (*s)[i*LocationLen : j*LocationLen]
}

func (s *LocationSlice) Append(bs []byte) {
	*s = append(*s, bs...)
}

func (s *LocationSlice) AppendLocation(location Location) {
	*s = append(*s, EncodeLocation(location)...)
}

func (s *LocationSlice) SetBytes(bs []byte) {
	*s = bs
}

func (s *LocationSlice) GetAllBytes() []byte {
	return *s
}

func (s *LocationSlice) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("LocationSlice[Len=%d]:\n", s.Len()))
	for i := 0; i < s.Len(); i++ {
		buf.WriteString(s.Get(i).String())
		buf.WriteByte('\n')
	}
	return buf.String()
}

// StringToLocation Generate a metaloc from an info string
func StringToLocation(info string) (Location, error) {
	location := strings.Split(info, "_")
	if len(location) < 8 {
		return nil, moerr.NewInternalErrorNoCtxf("bad location format: %v", info)
	}
	num, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		return nil, err
	}
	alg, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		return nil, err
	}
	offset, err := strconv.ParseUint(location[3], 10, 32)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseUint(location[4], 10, 32)
	if err != nil {
		return nil, err
	}
	osize, err := strconv.ParseUint(location[5], 10, 32)
	if err != nil {
		return nil, err
	}
	rows, err := strconv.ParseUint(location[6], 10, 32)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseUint(location[7], 10, 32)
	if err != nil {
		return nil, err
	}
	extent := NewExtent(uint8(alg), uint32(offset), uint32(size), uint32(osize))
	uid, err := types.ParseUuid(location[0])
	if err != nil {
		return nil, err
	}
	name := BuildObjectName(&uid, uint16(num))
	return BuildLocation(name, extent, uint32(rows), uint16(id)), nil
}
