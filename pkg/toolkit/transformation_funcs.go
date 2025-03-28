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

package toolkit

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// TruncateDate - truncate date till the provided part of date
func TruncateDate(part *string, t *time.Time) (*time.Time, error) {
	var month time.Month = 1
	var day = 1
	var year, hour, minute, second, nano int
	switch *part {
	case "nano":
		nano = t.Nanosecond()
		fallthrough
	case "second":
		second = t.Second()
		fallthrough
	case "minute":
		minute = t.Minute()
		fallthrough
	case "hour":
		hour = t.Hour()
		fallthrough
	case "day":
		day = t.Day()
		fallthrough
	case "month":
		month = t.Month()
		fallthrough
	case "year":
		year = t.Year()
	default:
		return nil, fmt.Errorf(`wrong part value "%s"`, *part)
	}
	res := time.Date(year, month, day, hour, minute, second, nano,
		t.Location(),
	)
	return &res, nil
}

//func daysInMonth(t time.Time) []int {
//	days := make([]int, 28, 31)
//	for i := range days {
//		days[i] = i + 1
//	}
//
//	m := t.Month()
//	// Roll to day 29
//	t = time.Date(t.Year(), t.Month(), 29, 0, 0, 0, 0, time.UTC)
//	for t.Month() == m {
//		days = append(days, t.Day())
//		t = t.AddDate(0, 0, 1)
//	}
//
//	return days
//}

func NoiseDateV2(r *rand.Rand, ratio time.Duration, t *time.Time) *time.Time {
	var multiplayer time.Duration = 1
	if r.Int31n(2) == 1 {
		multiplayer = -1
	}

	dur := r.Int63n(int64(ratio))

	res := t.Add(time.Duration(dur) * multiplayer)
	return &res
}

func NoiseFloat(r *rand.Rand, ratio float64, value float64, precision int) float64 {
	rndRatio := r.Float64() * ratio
	negative := r.Int63n(2) == 1
	if negative {
		rndRatio = rndRatio * -1
	}
	res := value + value*rndRatio
	return Round(precision, res)
}

func NoiseInt(r *rand.Rand, ratio float64, value int64) int64 {
	ratio = r.Float64() * ratio
	negative := r.Int63n(2) == 1
	if negative {
		ratio = ratio * -1
	}
	return value + int64(float64(value)*ratio)
}

func RandomBool(r *rand.Rand) bool {
	return r.Int63n(2) == 1
}

func RandomDate(r *rand.Rand, min, max *time.Time) *time.Time {
	delta := time.Duration(r.Int63n(int64(max.Sub(*min))))
	res := min.Add(delta)
	return &res
}

func RandomFloat(r *rand.Rand, min, max float64, precision int) float64 {
	res := min + r.Float64()*(max-min)
	return Round(precision, res)
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func Round(precision int, num float64) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func RandomInt(r *rand.Rand, min, max int64) int64 {
	return r.Int63n(max-min) + min
}

func RandomString(randGen *rand.Rand, minLength, maxLength int64, symbols []rune, buf []rune) string {
	length := maxLength
	if minLength != maxLength {
		length = minLength + randGen.Int63n(maxLength-minLength)
	}

	for i := int64(0); i < length; i++ {
		buf[i] = symbols[rand.Int63n(maxLength)%int64(len(symbols))]
	}
	return string(buf[:length])
}
