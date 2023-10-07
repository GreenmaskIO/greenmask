package toolkit

import "errors"

type Table struct {
	Schema      string       `json:"schema"`
	Name        string       `json:"name"`
	Oid         Oid          `json:"oid"`
	Columns     []*Column    `json:"columns"`
	Constraints []Constraint `json:"-"`
}

func (t *Table) Validate() error {
	if t.Schema == "" {
		return errors.New("empty table schema")
	}
	if t.Name == "" {
		return errors.New("empty table name")
	}
	if t.Oid == 0 {
		return errors.New("empty table oid")
	}
	if len(t.Columns) == 0 {
		return errors.New("empty table columns")
	}

	return nil
}
