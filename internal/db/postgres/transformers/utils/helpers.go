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
