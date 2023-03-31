package masker

type Masker interface {
	Mask(attributeValue string) (string, error)
}
