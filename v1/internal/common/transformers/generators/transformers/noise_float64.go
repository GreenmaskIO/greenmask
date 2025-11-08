// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformers

import (
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
)

const (
	noiseFloat64TransformerFloatSize = 8
	noiseFloat64TransformerSignByte  = 1
)

type NoiseFloat64Limiter struct {
	MinValue  float64
	MaxValue  float64
	Precision int
}

func NewNoiseFloat64Limiter(minVal, maxVal float64, precision int) (*NoiseFloat64Limiter, error) {
	if minVal >= maxVal {
		return nil, ErrWrongLimits
	}
	return &NoiseFloat64Limiter{
		MinValue:  minVal,
		MaxValue:  maxVal,
		Precision: precision,
	}, nil
}

func (ni *NoiseFloat64Limiter) Limit(v float64) float64 {
	if v < ni.MinValue {
		return ni.MinValue
	}
	if v > ni.MaxValue {
		return ni.MaxValue
	}
	return Round(ni.Precision, v)
}

type NoiseFloat64Transformer struct {
	generator  generators.Generator
	limiter    *NoiseFloat64Limiter
	byteLength int
	minRatio   float64
	maxRatio   float64
}

func NewNoiseFloat64Transformer(limiter *NoiseFloat64Limiter, minRatio, maxRatio float64) *NoiseFloat64Transformer {
	return &NoiseFloat64Transformer{
		limiter:    limiter,
		byteLength: noiseFloat64TransformerFloatSize + noiseFloat64TransformerSignByte,
		minRatio:   minRatio,
		maxRatio:   maxRatio,
	}
}

func (nt *NoiseFloat64Transformer) Transform(l *NoiseFloat64Limiter, original float64) (float64, error) {
	limiter := nt.limiter
	if l != nil {
		limiter = l
	}

	resBytes, err := nt.generator.Generate([]byte(fmt.Sprintf("%f", original)))
	if err != nil {
		return 0, err
	}

	randFloat := float64(int64(generators.BuildUint64FromBytes(resBytes[:8]))) / (1 << 63)
	if randFloat < 0 {
		randFloat = -randFloat
	}

	multiplayer := nt.minRatio + randFloat*(nt.maxRatio-nt.minRatio)

	negative := resBytes[8]%2 == 0
	if negative && multiplayer > 0 || !negative && multiplayer < 0 {
		multiplayer = -multiplayer
	}

	res := original + original*multiplayer

	if limiter != nil {
		res = limiter.Limit(res)
	}

	return res, nil
}

func (nt *NoiseFloat64Transformer) GetRequiredGeneratorByteLength() int {
	return nt.byteLength
}

func (nt *NoiseFloat64Transformer) SetGenerator(g generators.Generator) error {
	if g.Size() < nt.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", nt.byteLength, g.Size())
	}
	nt.generator = g
	return nil
}
