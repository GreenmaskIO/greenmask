package parameters

import (
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
)

// TODO:
//		Add tests for:
// 			1. Custom Unmarshaler function execution for ColumnValue and Scan
//  		2. Test cast template and cast functions for it
//			3. Test defaultValue caching after decoding - defaultValueScanned and defaultValueGot
//			4. Test default values behaviour when dynamic value IsNull
//		Implement:
//			1. Smart scanning - it must be possible scan compatible types values like int32 into int64. Add feature that
//			   allows to scan not pointer value into pointer receiver

type DynamicParameterContext struct {
	column       *models.Column
	linkedColumn *models.Column
	*template.RecordContextReadOnly
}

func NewDynamicParameterContext(column *models.Column) *DynamicParameterContext {
	return &DynamicParameterContext{
		column:                column,
		RecordContextReadOnly: template.NewRecordContextReadOnly(),
	}
}

func (dpc *DynamicParameterContext) setLinkedColumn(linkedColumn *models.Column) {
	dpc.linkedColumn = linkedColumn
}

func (dpc *DynamicParameterContext) setRecord(r commonininterfaces.Recorder) {
	dpc.RecordContextReadOnly.SetRecord(r)
}

func (dpc *DynamicParameterContext) GetColumnType() string {
	return dpc.column.TypeName
}

func (dpc *DynamicParameterContext) GetValue() (any, error) {
	return dpc.GetColumnValue(dpc.column.Name)
}

func (dpc *DynamicParameterContext) GetRawValue() (any, error) {
	return dpc.GetRawColumnValue(dpc.column.Name)
}

func (dpc *DynamicParameterContext) EncodeValue(v any) (any, error) {
	if dpc.linkedColumn == nil {
		return nil, fmt.Errorf("unable to encode not linked prameter use .EncodeValueByColumn or EncodeValueByType intead")
	}
	return dpc.EncodeValueByColumn(dpc.linkedColumn.Name, v)
}

func (dpc *DynamicParameterContext) DecodeValue(v any) (any, error) {
	if dpc.linkedColumn == nil {
		return nil, fmt.Errorf("unable to decode not linked prameter use .DecodeValueByColumn or DecodeValueByType intead")
	}
	return dpc.DecodeValueByColumn(dpc.linkedColumn.TypeName, v)
}
