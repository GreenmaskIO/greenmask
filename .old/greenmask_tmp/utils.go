package transformers

import (
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
)

var (
	TransformerMap = map[string]utils.TransformerMeta{
		transformers.ReplaceTransformerName:       transformers.ReplaceTransformerMeta,
		transformers.RegexpReplaceTransformerName: transformers.RegexpReplaceTransformerMeta,
		RandomUuidTransformerName:                 RandomUuidTransformerMeta,
		SetNullTransformerName:                    SetNullTransformerMeta,
		transformers.RandomDateTransformerName:    transformers.RandomDateTransformerMeta,
		RandomIntTransformerName:                  RandomIntTransformerMeta,
		RandomFloatTransformerName:                RandomFloatTransformerMeta,
		RandomStringTransformerName:               RandomStringTransformerMeta,
		RandomBoolTransformerName:                 RandomBoolTransformerMeta,
		NoiseDateTransformerName:                  NoiseDateTransformerMeta,
		NoiseIntTransformerName:                   NoiseIntTransformerMeta,
		NoiseFloatTransformerName:                 NoiseFloatTransformerMeta,
		JsonTransformerName:                       JsonTransformerMeta,
		transformers.MaskingTransformerName:       transformers.MaskingTransformerMeta,
		HashTransformerName:                       HashTransformerMeta,
	}
)
