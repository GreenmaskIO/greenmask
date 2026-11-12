// Copyright 2026 Greenmask
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
	"time"
	// Embedded IANA database: time.LoadLocation must work on Windows and in minimal images.
	_ "time/tzdata"

	"github.com/jackc/pgx/v5/pgtype"
)

// registerTimestamptzLocation registers new timestamptz types decoding into loc.
// Registered types are new objects: mutating the ones from pgx's shared default map would affect every map.
func registerTimestamptzLocation(m *pgtype.Map, loc *time.Location) {
	tz := &pgtype.Type{
		Name: "timestamptz", OID: pgtype.TimestamptzOID, Codec: &pgtype.TimestamptzCodec{ScanLocation: loc},
	}
	m.RegisterType(tz)
	m.RegisterType(&pgtype.Type{
		Name: "_timestamptz", OID: pgtype.TimestamptzArrayOID, Codec: &pgtype.ArrayCodec{ElementType: tz},
	})
	rng := &pgtype.Type{Name: "tstzrange", OID: pgtype.TstzrangeOID, Codec: &pgtype.RangeCodec{ElementType: tz}}
	m.RegisterType(rng)
	m.RegisterType(&pgtype.Type{
		Name: "_tstzrange", OID: pgtype.TstzrangeArrayOID, Codec: &pgtype.ArrayCodec{ElementType: rng},
	})
	m.RegisterType(&pgtype.Type{
		Name: "tstzmultirange", OID: pgtype.TstzmultirangeOID, Codec: &pgtype.MultirangeCodec{ElementType: rng},
	})
}
