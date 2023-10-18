package utils

import (
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func GetAffectedAttributes(driver *toolkit.Driver, attributes ...string) ([]int, map[int]string, error) {
	var attributeIdxs []int
	var attributeNames map[int]string
	if len(attributes) > 0 {
		attributeIdxs = make([]int, 0, len(attributes))
		attributeNames = make(map[int]string, len(attributes))
		for _, name := range attributes {
			idx, _, ok := driver.GetColumnByName(name)
			if !ok {
				return nil, nil, fmt.Errorf(`column "%s" is not found`, name)
			}
			attributeIdxs = append(attributeIdxs, idx)
			attributeNames[idx] = name
		}
	} else {
		attributeIdxs = make([]int, 0, len(driver.Table.Columns))
		attributeNames = make(map[int]string, len(driver.Table.Columns))
		for _, c := range driver.Table.Columns {
			idx, _, ok := driver.GetColumnByName(c.Name)
			if !ok {
				return nil, nil, fmt.Errorf(`column "%s" is not found`, c.Name)
			}
			attributeIdxs = append(attributeIdxs, idx)
			attributeNames[idx] = c.Name
		}
	}
	return attributeIdxs, attributeNames, nil
}

func NewApi(mode string, transferringColumns []int, affectedColumns []int) (InteractionApi, error) {
	var err error
	var api InteractionApi

	if len(affectedColumns) == 0 {
		return nil, fmt.Errorf("affected columns cannot be empty")
	}

	switch mode {
	case toolkit.JsonModeName:
		api, err = NewJsonApi(transferringColumns, affectedColumns)
		if err != nil {
			return nil, fmt.Errorf("error initializing json api: %w", err)
		}
	case toolkit.TextModeName:
		if len(affectedColumns) > 1 || len(transferringColumns) > 1 {
			return nil,
				fmt.Errorf(
					"use another interaction format (json or csv): text intearaction formats supports only 1 "+
						"attribute peer nullRecord: got transferring %d affected %d",
					len(transferringColumns), len(affectedColumns),
				)
		}

		var needSkip bool
		if len(transferringColumns) == 0 {
			needSkip = true
		}
		api, err = NewTextApi(affectedColumns[0], needSkip)
	default:
		return nil, fmt.Errorf("unknown interaction API: %s", mode)
	}
	return api, nil
}
