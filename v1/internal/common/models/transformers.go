package models

type TransformationConfig []TableConfig

func NewTransformationConfig(tables []TableConfig) TransformationConfig {
	return tables
}

type Transformers []TransformerConfig
