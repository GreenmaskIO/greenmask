package transformers

import (
	"fmt"
	"time"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/transformers/utils"
)

var (
	TransformerMap = map[string]utils.TransformerMeta{
		ReplaceTransformerName:       ReplaceTransformerMeta,
		RegexpReplaceTransformerName: RegexpReplaceTransformerMeta,
		RandomUuidTransformerName:    RandomUuidTransformerMeta,
		SetNullTransformerName:       SetNullTransformerMeta,
		RandomDateTransformerName:    RandomDateTransformerMeta,
		RandomIntTransformerName:     RandomIntTransformerMeta,
		RandomFloatTransformerName:   RandomFloatTransformerMeta,
		RandomStringTransformerName:  RandomStringTransformerMeta,
		RandomBoolTransformerName:    RandomBoolTransformerMeta,
		NoiseDateTransformerName:     NoiseDateTransformerMeta,
		NoiseIntTransformerName:      NoiseIntTransformerMeta,
		NoiseFloatTransformerName:    NoiseFloatTransformerMeta,
		JsonTransformerName:          JsonTransformerMeta,
		MaskingTransformerName:       MaskingTransformerMeta,
		HashTransformerName:          HashTransformerMeta,
	}
)

// TruncateDate - truncate date till the provided part of date
func truncateDate(t *time.Time, part *string) time.Time {
	// TODO: You should optimize this function or find another way to implement
	var month time.Month = 1
	var day = 1
	var year, hour, minute, second, nano int
	switch *part {
	case "nano":
		nano = t.Nanosecond()
		fallthrough
	case "second":
		second = t.Second()
		fallthrough
	case "minute":
		minute = t.Minute()
		fallthrough
	case "hour":
		hour = t.Hour()
		fallthrough
	case "day":
		day = t.Day()
		fallthrough
	case "month":
		month = t.Month()
		fallthrough
	case "year":
		year = t.Year()
	default:
		panic(fmt.Sprintf(`wrong Truncate value "%s"`, *part))
	}
	return time.Date(year, month, day, hour, minute, second, nano,
		t.Location(),
	)
}
