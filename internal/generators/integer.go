package generators

type IntegerGenerator struct {
	g Generator
}

func NewIntegerTransformer() *IntegerGenerator {
	return &IntegerGenerator{}
}

func (ig *IntegerGenerator) Transform(original []byte) ([]byte, error) {

}
