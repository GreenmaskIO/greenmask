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
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
)

// InteractionApi - API for interaction with Cmd transformer. It must implement context cancellation, RW timeouts,
// encode-decode operations, extracting DTO and assigning received DTO to the toolkit.Record
type InteractionApi interface {
	// SetWriter - assign writer
	SetWriter(w io.Writer)
	// SetReader - assign reader
	SetReader(r io.Reader)
	// GetRowDriverFromRecord - get from toolkit.Record all the required attributes as a toolkit.RowDriver instance
	GetRowDriverFromRecord(r *Record) (RowDriver, error)
	// SetRowDriverToRecord - set transformed toolkit.RowDriver to the toolkit.Record
	SetRowDriverToRecord(rd RowDriver, r *Record) error
	// Encode - write encoded data with \n symbol in the end into io.Writer
	Encode(ctx context.Context, row RowDriver) error
	// Decode - read data with new line from io.Reader and encode to toolkit.RowDriver
	Decode(ctx context.Context) (RowDriver, error)
	// Clean - clean cached Record
	Clean()
}

func NewApi(rowDriverParams *DriverParams, transferringColumns []*Column, affectedColumns []*Column, driver *Driver) (InteractionApi, error) {
	var err error
	var api InteractionApi

	if len(affectedColumns) == 0 {
		return nil, fmt.Errorf("affected columns cannot be empty")
	}

	switch rowDriverParams.Name {
	case JsonModeName:
		api, err = NewJsonApi(transferringColumns, affectedColumns, rowDriverParams)
		if err != nil {
			return nil, fmt.Errorf("error initializing json api: %w", err)
		}
	case TextModeName:
		if len(affectedColumns) > 1 || len(transferringColumns) > 1 {
			return nil,
				fmt.Errorf(
					"use another interaction api (json or csv): text intearaction api supports only 1 "+
						"attribute in the payload: got transferring %d affected %d",
					len(transferringColumns), len(affectedColumns),
				)
		}

		var needSkip bool
		if len(transferringColumns) == 0 {
			needSkip = true
		}
		api, err = NewTextApi(affectedColumns[0].Idx, needSkip)
		if err != nil {
			return nil, fmt.Errorf("error initializing text api: %w", err)
		}
	case CsvModeName:
		api = NewCsvApi(transferringColumns, affectedColumns, driver, rowDriverParams)
	default:
		return nil, fmt.Errorf("unknown interaction API: %s", rowDriverParams.Name)
	}
	log.Debug().Str("driver", rowDriverParams.Name).Msg("debug interaction driver")
	return api, nil
}

func GetAffectedAndTransferringColumns(parameters map[string]Parameterizer, driver *Driver) (
	affectedColumnsIdx []*Column, transferringColumnsIdx []*Column, err error,
) {
	for _, p := range parameters {
		if p.GetDefinition().IsColumn {
			v, err := p.Value()
			if err != nil {
				return nil, nil, fmt.Errorf("error getting parameter value: %w", err)
			}
			columnName, ok := v.(string)
			if !ok {
				return nil, nil, fmt.Errorf("unable to perform cast of column parameter value from any to *string")
			}

			_, c, ok := driver.GetColumnByName(columnName)
			if !ok {
				return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
			}
			if p.GetDefinition().ColumnProperties != nil {
				if p.GetDefinition().ColumnProperties.Affected {
					affectedColumnsIdx = append(affectedColumnsIdx, c)
				}
			} else {
				affectedColumnsIdx = append(affectedColumnsIdx, c)

			}

			if p.GetDefinition().ColumnProperties != nil {
				if !p.GetDefinition().ColumnProperties.SkipOriginalData {
					transferringColumnsIdx = append(transferringColumnsIdx, c)
				}
			} else {
				transferringColumnsIdx = append(transferringColumnsIdx, c)
			}
		}
	}
	return affectedColumnsIdx, transferringColumnsIdx, nil

}
