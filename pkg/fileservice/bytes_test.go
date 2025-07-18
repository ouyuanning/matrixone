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

package fileservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	t.Run("Bytes without refs", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
			refs:        1,
		}
		bs.Release()
	})
}

func TestBytesPanic(t *testing.T) {
	bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
	assert.Nil(t, err)
	bs := &Bytes{
		bytes:       bytes,
		deallocator: deallocator,
		refs:        1,
	}

	released := bs.Release()
	assert.Equal(t, released, true)

	assert.Panics(t, func() { bs.Release() }, "Bytes.Release panic()")
	assert.Panics(t, func() { bs.Size() }, "Bytes.Size panic()")
	assert.Panics(t, func() { bs.Bytes() }, "Bytes.Bytes panic()")
	assert.Panics(t, func() { bs.Slice(0) }, "Bytes.Slice panic()")
	assert.Panics(t, func() { bs.Retain() }, "Bytes.Retain panic()")

}
