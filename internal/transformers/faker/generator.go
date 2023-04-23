package faker

import "golang.org/x/exp/rand"

type RandomFakerGenerator struct {
}

func (rd *RandomFakerGenerator) Intn(n int) int {
	return rand.Intn(n)
}

func (rd *RandomFakerGenerator) Int() int {
	return rand.Int()
}
