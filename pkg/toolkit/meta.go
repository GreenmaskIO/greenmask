package toolkit

type Meta struct {
	Table               *Table                 `json:"table"`
	Parameters          map[string]ParamsValue `json:"parameters"`
	Types               []*Type                `json:"types"`
	ColumnTypeOverrides map[string]string      `json:"column_type_overrides"`
}
