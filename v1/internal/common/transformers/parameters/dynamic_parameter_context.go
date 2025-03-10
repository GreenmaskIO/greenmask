package parameters

import (
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// TODO:
//		Add tests for:
// 			1. Custom Unmarshaller function execution for ColumnValue and Scan
//  		2. Test cast template and cast functions for it
//			3. Test defaultValue caching after decoding - defaultValueScanned and defaultValueGot
//			4. Test default values behaviour when dynamic value IsNull
//		Implement:
//			1. Smart scanning - it must be possible scan compatible types values like int32 into int64. Add feature that
//			   allows to scan not pointer value into pointer receiver

type DynamicParameterContext struct {
	column       *models.Column
	linkedColumn *models.Column
	rc           *RecordContext
}

func NewDynamicParameterContext(column *models.Column) *DynamicParameterContext {
	if column == nil {
		panic("column cannot be nil")
	}

	return &DynamicParameterContext{
		column: column,
		rc:     &RecordContext{},
	}
}

func (dpc *DynamicParameterContext) setLinkedColumn(linkedColumn *models.Column) {
	dpc.linkedColumn = linkedColumn
}

func (dpc *DynamicParameterContext) setRecord(r record) {
	dpc.rc.SetRecord(r)
}

func (dpc *DynamicParameterContext) GetColumnType() string {
	return dpc.column.Type
}

func (dpc *DynamicParameterContext) GetValue() (any, error) {
	return dpc.rc.GetColumnValue(dpc.column.Name)
}

func (dpc *DynamicParameterContext) GetRawValue() (any, error) {
	return dpc.rc.GetRawColumnValue(dpc.column.Name)
}

func (dpc *DynamicParameterContext) GetColumnValue(name string) (any, error) {
	return dpc.rc.GetColumnValue(name)
}

func (dpc *DynamicParameterContext) GetRawColumnValue(name string) (any, error) {
	return dpc.rc.GetRawColumnValue(name)
}

func (dpc *DynamicParameterContext) EncodeValue(v any) (any, error) {
	if dpc.linkedColumn == nil {
		return nil, fmt.Errorf("unable to encode not linked prameter use .EncodeValueByColumn or EncodeValueByType intead")
	}
	return dpc.rc.EncodeValueByColumn(dpc.linkedColumn.Name, v)
}

func (dpc *DynamicParameterContext) DecodeValue(v any) (any, error) {
	if dpc.linkedColumn == nil {
		return nil, fmt.Errorf("unable to decode not linked prameter use .DecodeValueByColumn or DecodeValueByType intead")
	}
	return dpc.rc.DecodeValueByColumn(dpc.linkedColumn.Type, v)
}

func (dpc *DynamicParameterContext) EncodeValueByColumn(name string, v any) (any, error) {
	return dpc.rc.EncodeValueByColumn(name, v)
}

func (dpc *DynamicParameterContext) DecodeValueByColumn(name string, v any) (any, error) {
	return dpc.rc.DecodeValueByColumn(name, v)
}

func (dpc *DynamicParameterContext) EncodeValueByType(name string, v any) (any, error) {
	return dpc.rc.EncodeValueByType(name, v)
}

func (dpc *DynamicParameterContext) DecodeValueByType(name string, v any) (any, error) {
	return dpc.rc.DecodeValueByType(name, v)
}
