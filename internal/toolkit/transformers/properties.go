package transformers

type Properties struct {
	Name               string             `json:"name"`
	Description        string             `json:"description"`
	TransformationType TransformationType `json:"transformationType,omitempty"`
	Extended           map[string]any     `json:"extended,omitempty"`
	//Validate           bool               `json:"validate,omitempty"`
	//IsCustom           bool               `json:"isCustom,omitempty"`
}

func MustNewProperties(name, description string, transformationType TransformationType) *Properties {
	p, err := NewProperties(name, description, transformationType)
	if err != nil {
		panic(err.Error())
	}
	return p
}

func NewProperties(name, description string, transformationType TransformationType) (*Properties, error) {
	if err := validateTransformation(transformationType); err != nil {
		return nil, err
	}

	return &Properties{
		Name:               name,
		Description:        description,
		TransformationType: transformationType,
		Extended:           make(map[string]any),
	}, nil
}

func (p *Properties) SetTransformationType(transformationType TransformationType) *Properties {
	if err := validateTransformation(transformationType); err != nil {
		panic(err.Error())
	}
	p.TransformationType = transformationType
	return p
}

func (p *Properties) AddExtended(name string, data any) *Properties {
	p.Extended[name] = data
	return nil
}
