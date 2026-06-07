package core

type TransformerRegistry interface {
	Get(name string) (TransformerProvisioner, bool)
}
