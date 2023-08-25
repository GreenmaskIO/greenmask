package transformers

type Table struct {
	Schema      string        `json:"schema"`
	Name        string        `json:"name"`
	Oid         uid32         `json:"oid"`
	Columns     []*Column     `json:"columns"`
	Constraints []*Constraint `json:"constraints"`
}
