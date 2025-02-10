package generators

import "fmt"

type Projector struct {
	generators []Generator
}

func NewProjector(generators ...Generator) *Projector {
	return &Projector{
		generators: generators,
	}
}

func (p *Projector) Generate(data []byte) (res []byte, err error) {
	res = data
	for idx, g := range p.generators {
		res, err = g.Generate(res)
		if err != nil {
			return nil, fmt.Errorf("error generating data using %d genrator: %w", idx, err)
		}
	}
	return res, nil
}

func (p *Projector) Size() int {
	return p.generators[len(p.generators)-1].Size()
}
