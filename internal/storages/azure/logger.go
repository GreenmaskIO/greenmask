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

package azure

import (
	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// setupLogging routes the Azure SDK's request/response logging into zerolog at
// debug level, mirroring the s3 storage LogWrapper so the underlying API calls
// can be explored when troubleshooting. It is a no-op unless the configured log
// level is debug.
//
// NOTE: unlike the aws-sdk logger (set per config), the azcore log listener is
// process-wide (a package-level singleton). greenmask uses a single storage per
// run, so the global scope is not an issue in practice. Auth headers and query
// parameters (e.g. SAS tokens) are redacted by the SDK before reaching the
// listener.
func setupLogging(logLevel string) {
	if logLevel != zerolog.LevelDebugValue {
		return
	}
	azlog.SetEvents(
		azlog.EventRequest,
		azlog.EventResponse,
		azlog.EventResponseError,
		azlog.EventRetryPolicy,
		azlog.EventLRO,
	)
	azlog.SetListener(func(event azlog.Event, msg string) {
		log.Debug().Str("class", string(event)).Msg(msg)
	})
}
