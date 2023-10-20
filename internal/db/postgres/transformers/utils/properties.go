package utils

type TransformerProperties struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	IsCustom    bool   `json:"is_custom"`
}

func NewTransformerProperties(
	name, description string,
) *TransformerProperties {
	return &TransformerProperties{
		Name:        name,
		Description: description,
	}
}
