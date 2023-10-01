package transformers

type TransformerProperties struct {
	Name        string
	Description string
	IsCustom    bool
}

func NewTransformerProperties(
	name, description string,
) *TransformerProperties {
	return &TransformerProperties{
		Name:        name,
		Description: description,
	}
}
