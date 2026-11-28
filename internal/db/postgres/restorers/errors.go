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

package restorers

// IgnoredError is a restoration error that does not stop the restore because
// exit-on-error is off. The restore carries on, and reports it when it ends so
// that greenmask exits with a non-zero code, as pg_restore does.
type IgnoredError struct {
	Err error
}

func (e *IgnoredError) Error() string {
	return e.Err.Error()
}

func (e *IgnoredError) Unwrap() error {
	return e.Err
}
