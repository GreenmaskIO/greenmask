package tableruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestProducer_Produce(t *testing.T) {
	tables := []commonmodels.Table{
		{
			Schema: "public",
			Name:   "test",
			Columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "id",
					TypeName: "integer",
					TypeOID:  0,
				},
				{
					Idx:      1,
					Name:     "title",
					TypeName: "text",
					TypeOID:  1,
				},
				{
					Idx:      2,
					Name:     "created_at",
					TypeName: "timestamp",
					TypeOID:  2,
				},
				{
					Idx:      3,
					Name:     "json_data",
					TypeName: "jsonb",
					TypeOID:  3,
				},
				{
					Idx:      4,
					Name:     "float_data",
					TypeName: "float8",
					TypeOID:  4,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(table commonmodels.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		_, newFunc := mocks.NewTransformerMock()

		tr := transformerutils.NewTransformerRegistry()
		tr.MustRegister(
			transformerutils.NewTransformerDefinition(
				transformerutils.NewTransformerProperties("Test", "test desc"),
				newFunc,
				parameters.MustNewParameterDefinition("Test", "test desc").
					SetIsColumn(
						parameters.NewColumnProperties().
							SetAllowedColumnTypes("int"),
					),
			),
		)

		p := NewProducer(tables, dumpQueries, nil, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		_, err := p.Produce(ctx, vc)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
	})
}
