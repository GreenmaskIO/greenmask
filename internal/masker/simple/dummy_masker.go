package simple

type DummyMasker struct {
}

func (doh *DummyMasker) Mask(attributeValue string) (string, error) {
	return "blah blah", nil
}
