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

package fileservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Test_NewAwsSDKv2(t *testing.T) {
	_, err := NewAwsSDKv2(context.Background(), ObjectStorageArguments{}, nil)
	assert.Error(t, err)

	ctx, cancel := context.WithTimeoutCause(context.Background(), 0, moerr.NewInternalErrorNoCtx("ut tester"+
		""))
	defer cancel()

	_, err = NewAwsSDKv2(ctx, ObjectStorageArguments{}, nil)
	assert.Error(t, err)
}
