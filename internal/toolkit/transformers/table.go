package transformers

type Table struct {
	Schema      string        `json:"schema"`
	Name        string        `json:"name"`
	Oid         Oid           `json:"oid"`
	Columns     []*Column     `json:"columns"`
	Constraints []*Constraint `json:"constraints"`
}
