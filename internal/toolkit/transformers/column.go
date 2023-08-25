package transformers

type Column struct {
	Name     string `json:"name"`
	TypeName string `json:"typeName"`
	TypeOid  uint32 `json:"typeOid"`
	Num      int    `json:"num"`
	NotNull  bool   `json:"notNull"`
	Length   int64  `json:"length"`
}
