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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// CMDRowDriver - API for interaction with Cmd transformer row representation.
type CMDRowDriver interface {
	// GetColumn - get raw []byte value by column idx.
	GetColumn(c *commonmodels.Column) (*commonmodels.ColumnRawValue, error)
	// SetColumn - set RawValue value by column idx to the current row.
	SetColumn(c *commonmodels.Column, v *commonmodels.ColumnRawValue) error
	// Clean - clean internal state.
	Clean()
	// Encode - encode current row to []byte.
	Encode() ([]byte, error)
	// Decode - decode []byte to current row.
	Decode(data []byte) error
}

// CMDProto - API for interaction with Cmd transformer. It must implement context cancellation, RW timeouts,
// encode-decode operations, extracting DTO and assigning received DTO to the toolkit.Record
type CMDProto interface {
	// Init - initialize interactor with io.Writer and io.Reader.
	Init(w io.Writer, r io.Reader) error
	// Send - send record to the Cmd transformer.
	Send(ctx context.Context, r commonininterfaces.Recorder) error
	// ReceiveAndApply - receive record from the Cmd transformer and apply received transferRecord to the record.
	ReceiveAndApply(ctx context.Context, r commonininterfaces.Recorder) error
}

var (
	errConflictingColumnMappings = errors.New("conflicting column mappings found")
	errNoAffectedColumns         = errors.New("no affected columns provided")
)

type ColumnMapping struct {
	Column *commonmodels.Column
	// Position - position in the message (for positional formats like CSV or JsonIndexes)
	Position int
}

func isColumnMappingConflicting(ms []*ColumnMapping) error {
	positions := make(map[int]struct{}, len(ms))
	for _, m := range ms {
		if _, ok := positions[m.Position]; ok {
			return fmt.Errorf(
				"position %d is mapped to multiple columns: %w",
				m.Position,
				errConflictingColumnMappings,
			)
		}
		positions[m.Position] = struct{}{}
	}
	return nil
}

func initJsonDriverBySettings(
	driverCfg JsonRowDriverConfig,
	transferredColumns []*ColumnMapping,
	receiveColumns []*ColumnMapping,
) (res CMDRowDriver, err error) {

	switch driverCfg.ColumnFormat {
	case JsonRowDriverColumnFormatByIndexes:
		switch driverCfg.DataFormat {
		case JsonRowDriverDataFormatBytes:
			res = NewJsonRecordWithAttrIndexes[*JsonAttrRawValueBytes](
				transferredColumns,
				receiveColumns,
				NewJsonAttrRawValueBytes,
			)
		case JsonRowDriverDataFormatText:
			res = NewJsonRecordWithAttrIndexes[*JsonAttrRawValueText](
				transferredColumns,
				receiveColumns,
				NewJsonAttrRawValueText,
			)
		default:
			panic(fmt.Sprintf("unsupported json row driver transferRecord format: %s", driverCfg.DataFormat))
		}
	case JsonRowDriverColumnFormatByNames:
		switch driverCfg.DataFormat {
		case JsonRowDriverDataFormatBytes:
			res = NewJsonRecordWithAttrNames[*JsonAttrRawValueBytes](NewJsonAttrRawValueBytes)
		case JsonRowDriverDataFormatText:
			res = NewJsonRecordWithAttrNames[*JsonAttrRawValueText](NewJsonAttrRawValueText)
		default:
			panic(fmt.Sprintf("unsupported json row driver transferRecord format: %s", driverCfg.DataFormat))
		}
	default:
		panic(fmt.Sprintf("unsupported json row driver column format: %s", driverCfg.ColumnFormat))
	}
	return res, nil
}

func NewProto(
	settings *RowDriverSetting,
	transferringColumns []*ColumnMapping,
	affectedColumns []*ColumnMapping,
) (CMDProto, error) {
	var (
		rowDriver CMDRowDriver
		err       error
	)

	if len(affectedColumns) == 0 {
		return nil, errNoAffectedColumns
	}

	if settings.IsPositionedAttributeFormat() {
		if err := isColumnMappingConflicting(transferringColumns); err != nil {
			return nil, fmt.Errorf("validate transferring columns: %w", err)
		}
	}

	switch settings.Name {
	case RowDriverNameJson:
		rowDriver, err = initJsonDriverBySettings(settings.JsonConfig, transferringColumns, affectedColumns)
		if err != nil {
			return nil, fmt.Errorf("init json row driver: %w", err)
		}
	case RowDriverNameText:
		rowDriver, err = NewTextRecord(transferringColumns, affectedColumns)
		if err != nil {
			return nil, fmt.Errorf("init text row driver: %w", err)
		}
	case RowDriverNameCSV:
		rowDriver, err = NewCSVRecord(affectedColumns, transferringColumns)
		if err != nil {
			return nil, fmt.Errorf("init csv row driver: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown CMD protocol: %s", settings.Name)
	}

	transferringCols := make([]*commonmodels.Column, len(transferringColumns))
	for i, c := range transferringColumns {
		transferringCols[i] = c.Column
	}
	affectedCols := make([]*commonmodels.Column, len(affectedColumns))
	for i, c := range affectedColumns {
		affectedCols[i] = c.Column
	}
	return NewDefaultCMDProto(rowDriver, transferringCols, affectedCols), nil
}
