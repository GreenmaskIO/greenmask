package transformers

type TransformerProperties struct {
	Name               string             `json:"name"`
	Description        string             `json:"description"`
	IsCustom           bool               `json:"isCustom"`
	TransformationType TransformationType `json:"transformationType,omitempty"`
	Extended           map[string]any     `json:"extended,omitempty"`
}

func MustNewTransformerProperties(
	name, description string, transformationType TransformationType,
) *TransformerProperties {
	p, err := NewTransformerProperties(name, description, transformationType)
	if err != nil {
		panic(err)
	}
	return p
}

func NewTransformerProperties(name, description string, transformationType TransformationType) (
	*TransformerProperties, error,
) {
	if err := validateTransformation(transformationType); err != nil {
		return nil, err
	}

	return &TransformerProperties{
		Name:               name,
		Description:        description,
		TransformationType: transformationType,
		Extended:           make(map[string]any),
	}, nil
}

func (p *TransformerProperties) SetTransformationType(transformationType TransformationType) *TransformerProperties {
	if err := validateTransformation(transformationType); err != nil {
		panic(err.Error())
	}
	p.TransformationType = transformationType
	return p
}

func (p *TransformerProperties) AddExtended(name string, data any) *TransformerProperties {
	p.Extended[name] = data
	return nil
}
