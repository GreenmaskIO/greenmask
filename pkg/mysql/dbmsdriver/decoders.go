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

package dbmsdriver

import (
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

// decodeInt strictly decodes a signed MySQL integer to int64. A signed integer
// column never exceeds the int64 range, so an out-of-range value is a real
// error, not a reason to silently widen to uint64 (which would make the decoded
// Go type depend on the row value instead of the column type).
func decodeInt(buf []byte) (any, error) {
	return strconv.ParseInt(string(buf), 10, 64)
}

// decodeUint strictly decodes an unsigned MySQL integer to uint64, for every
// row regardless of magnitude — so one unsigned column always yields uint64.
func decodeUint(buf []byte) (any, error) {
	return strconv.ParseUint(string(buf), 10, 64)
}

func decodeBool(buf []byte) (any, error) {
	switch string(buf) {
	case "1", "true", "TRUE", "True":
		return true, nil
	case "0", "false", "FALSE", "False":
		return false, nil
	default:
		return nil, fmt.Errorf("cannot decode %q as bool", buf)
	}
}

func decodeEnum(buf []byte) (any, error) {
	return string(buf), nil
}

func decodeDecimal(buf []byte) (any, error) {
	return decimal.NewFromString(string(buf))
}

func decodeBit(buf []byte) (any, error) {
	return strconv.ParseInt(string(buf), 10, 8)
}

func decodeTime(buf []byte) (any, error) {
	t, err := time.Parse("15:04:05", string(buf))
	if err != nil {
		return 0, err
	}
	return time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second, nil
}
