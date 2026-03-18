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

package dump

import (
	"context"
	"io"

	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

type CmdRunnerMock struct {
	mock.Mock
}

func (m *CmdRunnerMock) ExecuteCmdAndForwardStdout(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *CmdRunnerMock) ExecuteCmdAndWriteStdout(ctx context.Context, w io.Writer) error {
	args := m.Called(ctx, w)
	return args.Error(0)
}

func (m *CmdRunnerMock) ExecuteCmd(ctx context.Context, w io.Writer, mode int) error {
	args := m.Called(ctx, w, mode)
	return args.Error(0)
}

type CmdProducerMock struct {
	mock.Mock
}

func (m *CmdProducerMock) Produce(executable string, args []string, env []string, stdin io.Reader) (utils.CmdRunnerInterface, error) {
	callArgs := m.Called(executable, args, env, stdin)
	return callArgs.Get(0).(utils.CmdRunnerInterface), callArgs.Error(1)
}
