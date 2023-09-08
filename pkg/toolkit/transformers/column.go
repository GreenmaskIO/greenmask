package transformers

type Column struct {
	Name     string `json:"name"`
	TypeName string `json:"typeName"`
	TypeOid  Oid    `json:"typeOid"`
	Num      AttNum `json:"num"`
	NotNull  bool   `json:"notNull"`
	Length   int    `json:"length"`
}
