package toolkit

type Column struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	TypeOid  Oid    `json:"type_oid"`
	Num      AttNum `json:"num"`
	NotNull  bool   `json:"not_null"`
	Length   int    `json:"length"`
}
