// Copyright 2025 Matrix Origin
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
	"io"
)

type ReaderWriterFileService interface {
	FileService

	// NewReader creates an io.ReadCloser for reading the content of the file
	NewReader(ctx context.Context, filePath string) (io.ReadCloser, error)

	// NewWriter creates an io.WriteCloser for writing to the file
	NewWriter(ctx context.Context, filePath string) (io.WriteCloser, error)
}
