package interfaces

import commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"

type ConfigEditor interface {
	EditConfig(input commonmodels.ConfigEditInput) []commonmodels.TableConfig
}
