// Copyright 2023 Greenmask
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

package utils

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// slowApi records the order of Encode/Decode calls; each call sleeps for the next configured delay
type slowApi struct {
	mu           sync.Mutex
	events       []string
	encodeDelays []time.Duration
	call         int
}

func (a *slowApi) log(e string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, e)
}

func (a *slowApi) SetWriter(io.Writer) {}
func (a *slowApi) SetReader(io.Reader) {}
func (a *slowApi) GetRowDriverFromRecord(*toolkit.Record) (toolkit.RowDriver, error) {
	return &toolkit.RawRecordText{}, nil
}
func (a *slowApi) SetRowDriverToRecord(rd toolkit.RowDriver, _ *toolkit.Record) error {
	if rd == nil {
		a.log("set-nil")
	}
	return nil
}
func (a *slowApi) Encode(context.Context, toolkit.RowDriver) error {
	a.mu.Lock()
	d := a.encodeDelays[a.call]
	a.call++
	a.mu.Unlock()
	time.Sleep(d)
	a.log("encode-end")
	return nil
}
func (a *slowApi) Decode(context.Context) (toolkit.RowDriver, error) {
	a.log("decode-start")
	return &toolkit.RawRecordText{}, nil
}
func (a *slowApi) Clean() {}

func TestCmdTransformerBase_Transform_late_signal_after_timeout(t *testing.T) {
	api := &slowApi{encodeDelays: []time.Duration{100 * time.Millisecond, 50 * time.Millisecond}}
	ctb := NewCmdTransformerBase("test", 0, 30*time.Millisecond, &toolkit.Driver{}, api)

	_, err := ctb.Transform(context.Background(), &toolkit.Record{})
	require.ErrorIs(t, err, ErrRowTransformationTimeout)
	// let the timed out Encode finish and send its signal
	time.Sleep(120 * time.Millisecond)

	ctb.RowTransformationTimeout = time.Second
	_, err = ctb.Transform(context.Background(), &toolkit.Record{})
	require.NoError(t, err)

	api.mu.Lock()
	defer api.mu.Unlock()
	// the second row must not start decoding before its own Encode finished
	require.Equal(t, []string{"encode-end", "encode-end", "decode-start"}, api.events)
}
