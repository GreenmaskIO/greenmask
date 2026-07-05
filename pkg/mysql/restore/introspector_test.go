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

package restore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// errSession is a core.DatabaseSession whose RunWithEngineResource always fails,
// exercising the introspector's error-wrapping path without a real database.
type errSession struct{ err error }

func (s errSession) Close(_ context.Context) error { return nil }
func (s errSession) RunWithOperationalDB(_ context.Context, _ func(context.Context, core.DB) error) error {
	return core.ErrEngineResourceNotSupported
}
func (s errSession) RunWithEngineResource(_ context.Context, _ func(context.Context, any) error) error {
	return s.err
}

func TestRestoreIntrospector_Introspect_queryError(t *testing.T) {
	r := &RestoreIntrospector{}
	_, err := r.Introspect(context.Background(), errSession{err: errors.New("connection refused")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query target server version")
	assert.Contains(t, err.Error(), "connection refused")
}
