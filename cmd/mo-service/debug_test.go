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

package main

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func Test_saveProfile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, dir)
	assert.Nil(t, err)
	defer fs.Close(ctx)
	globalEtlFS = fs
	saveCpuProfile(time.Second)
	saveProfiles()
}

func Test_saveProfile2(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)
	defer fs.Close(ctx)
	globalEtlFS = fs
	saveCpuProfile(time.Second)
}

func Test_saveProfile3(t *testing.T) {
	ctx := context.Background()
	sigs := make(chan os.Signal, 1)
	close(sigs)
	*profileInterval = time.Second * 10
	dir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, dir)
	assert.Nil(t, err)
	defer fs.Close(ctx)
	globalEtlFS = fs
	saveProfilesLoop(sigs)
}

func Test_saveProfile4(t *testing.T) {
	saveProfileWithType("cpu", func(writer io.Writer) error {
		return context.DeadlineExceeded
	})
}

func Test_saveMallocProfile5(t *testing.T) {
	globalEtlFS = testutil.NewFS(t)
	saveMallocProfile()
}
