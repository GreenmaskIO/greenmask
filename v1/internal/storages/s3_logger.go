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

package storages

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type LogWrapper struct {
	logger *zerolog.Logger
}

func (lw LogWrapper) Log(objs ...interface{}) {
	event := log.Debug()
	for idx, o := range objs {
		event.Any(fmt.Sprintf("%d", idx), o)
	}
	event.Msg("s3 storage logging")
}
