package models

import (
	"fmt"
	"strings"
)

type TableFilter struct {
	Name   string
	Schema string
}

func NewTableFilter(name, schema string) TableFilter {
	return TableFilter{
		Name:   name,
		Schema: schema,
	}
}

func NewTableFilterItemFromString(fullName string) (TableFilter, error) {
	var schema, name string
	parts := strings.Split(fullName, ".")
	if len(parts) == 2 {
		schema = parts[0]
		name = parts[1]
	} else if len(parts) == 1 {
		name = parts[0]
	} else {
		return TableFilter{}, fmt.Errorf("invalid table full name: %s", fullName)
	}
	return TableFilter{
		Name:   name,
		Schema: schema,
	}, nil
}

type TaskProducerFilter struct {
	Tables []TableFilter
}

func (f *TaskProducerFilter) IsAllowed(table Table) bool {
	if len(f.Tables) == 0 {
		return true
	}
	for _, tf := range f.Tables {
		if tf.Name == table.Name && (tf.Schema == "" || tf.Schema == table.Schema) {
			return true
		}
	}
	return false
}
