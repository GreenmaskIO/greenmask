// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// RowStreamReader - represents a stream reader from DBMS.
type RowStreamReader interface {
	Open(ctx context.Context) error
	// ReadRow - returns row from stream until commonmodels.ErrEndOfStream
	// error is returned.
	ReadRow(ctx context.Context) ([][]byte, error)
	Close(ctx context.Context) error
	// DebugInfo - info about resource that is dumping.
	// It contains map metadata with meta keys from commonmodels.
	DebugInfo() map[string]any
}

// RowStreamWriter -
type RowStreamWriter interface {
	Open(ctx context.Context) error
	WriteRow(ctx context.Context, row [][]byte) error
	Close(ctx context.Context) error
	// Stat - returns a statistic of written and compressed data
	// and some additional info.
	Stat() models.ObjectStat
}
