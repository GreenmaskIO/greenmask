package generators

// Dependencies:
//  1. Int generator
//	   * Hash function -> has an input
//     * Random gen
//

// We don't know the byte length in the output, min value, max value
type Generator interface {
	Generate([]byte) ([]byte, error)
	Size() int
}
