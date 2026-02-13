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
	noiseInt64TransformerFloatSize = 8
	noiseInt64TransformerSignByte  = 1
)

type NoiseInt64Limiter struct {
	MinValue int64
	MaxValue int64
}

func NewNoiseInt64Limiter(minVal, maxVal int64) (*NoiseInt64Limiter, error) {
	if minVal >= maxVal {
		return nil, ErrWrongLimits
	}
	return &NoiseInt64Limiter{
		MinValue: minVal,
		MaxValue: maxVal,
	}, nil
}

func (ni *NoiseInt64Limiter) Limit(v int64) int64 {
	if v < ni.MinValue {
		return ni.MinValue
	}
	if v > ni.MaxValue {
		return ni.MaxValue
	}
	return v
}

type NoiseInt64Transformer struct {
	generator  generators.Generator
	limiter    *NoiseInt64Limiter
	byteLength int
	minRatio   float64
	maxRatio   float64
}

func NewNoiseInt64Transformer(limiter *NoiseInt64Limiter, minRatio, maxRatio float64) (*NoiseInt64Transformer, error) {
	return &NoiseInt64Transformer{
		limiter:    limiter,
		byteLength: noiseInt64TransformerFloatSize + noiseInt64TransformerSignByte,
		minRatio:   minRatio,
		maxRatio:   maxRatio,
	}, nil
}

func (ig *NoiseInt64Transformer) Transform(l *NoiseInt64Limiter, original int64) (int64, error) {
	limiter := ig.limiter
	if l != nil {
		limiter = l
	}

	resBytes, err := ig.generator.Generate([]byte(fmt.Sprintf("%d", original)))
	if err != nil {
		return 0, err
	}

	randFloat := float64(int64(generators.BuildUint64FromBytes(resBytes[:8]))) / (1 << 63)
	if randFloat < 0 {
		randFloat = -randFloat
	}

	multiplayer := ig.minRatio + randFloat*(ig.maxRatio-ig.minRatio)

	negative := resBytes[8]%2 == 0
	if negative && multiplayer > 0 || !negative && multiplayer < 0 {
		multiplayer = -multiplayer
	}

	res := original + int64(float64(original)*multiplayer)

	if limiter != nil {
		res = limiter.Limit(res)
	}

	return res, nil
}

func (ig *NoiseInt64Transformer) GetRequiredGeneratorByteLength() int {
	return ig.byteLength
}

func (ig *NoiseInt64Transformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ig.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ig.byteLength, g.Size())
	}
	ig.generator = g
	return nil
}
