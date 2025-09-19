package dumpers

import (
	"fmt"
	"maps"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func getUniqueDumpTaskID(dumperType string, meta map[string]any) string {
	tableSchema, ok := meta[commonmodels.MetaKeyTableSchema].(string)
	if !ok {
		tableSchema = "!!!UNKNOWN!!!"
	}
	tableName, ok := meta[commonmodels.MetaKeyTableName].(string)
	if !ok {
		tableName = "!!!UNKNOWN!!!"
	}
	meta = maps.Clone(meta)
	return fmt.Sprintf(
		"%s___%s.%s", dumperType, tableSchema, tableName,
	)
}
