package custom_transformers

type Column struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	TypeOid  string `json:"type_oid"`
	NotNull  bool   `json:"not_null"`
	Length   bool   `json:"length"`
}

type Table struct {
	Schema  string   `json:"schema"`
	Name    string   `json:"name"`
	Oid     int      `json:"oid"`
	Columns []Column `json:"columns"`
}

type Meta struct {
	Table Table `json:"table"`
}
