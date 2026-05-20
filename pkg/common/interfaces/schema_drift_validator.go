package interfaces

import commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"

type SchemaDriftValidator interface {
	Compare(input commonmodels.SchemaDriftValidatorInput) commonmodels.DiffResult
}
