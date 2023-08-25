package transformers

type Meta struct {
	Table      *Table         `json:"table"`
	Parameters map[string]any `json:"parameters"`
}
