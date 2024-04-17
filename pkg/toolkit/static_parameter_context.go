package toolkit

import "fmt"

type StaticParameterContext struct {
	rc               *RecordContext
	linkedColumnName string
}

func NewStaticParameterContext(d *Driver, linkedColumnName string) *StaticParameterContext {
	dummyRecord := NewRecord(d)
	rc := NewRecordContext()
	rc.SetRecord(dummyRecord)
	return &StaticParameterContext{
		rc:               rc,
		linkedColumnName: linkedColumnName,
	}
}

func (spc *StaticParameterContext) GetColumnType(name string) (string, error) {
	return spc.GetColumnType(name)
}

func (spc *StaticParameterContext) EncodeValueByColumn(name string, v any) (any, error) {
	return spc.rc.EncodeValueByColumn(name, v)
}

func (spc *StaticParameterContext) DecodeValueByColumn(name string, v any) (any, error) {
	return spc.rc.DecodeValueByColumn(name, v)
}

func (spc *StaticParameterContext) EncodeValueByType(name string, v any) (any, error) {
	return spc.rc.EncodeValueByType(name, v)
}

func (spc *StaticParameterContext) DecodeValueByType(name string, v any) (any, error) {
	return spc.rc.DecodeValueByType(name, v)
}

func (spc *StaticParameterContext) EncodeValue(v any) (any, error) {
	if spc.linkedColumnName == "" {
		return nil, fmt.Errorf("linked column name is not set use .EncodeValueByType or .EncodeValueByColumn instead")
	}
	return spc.rc.EncodeValueByColumn(spc.linkedColumnName, v)
}

func (spc *StaticParameterContext) DecodeValue(v any) (any, error) {
	if spc.linkedColumnName == "" {
		return nil, fmt.Errorf("linked column name is not set use .DecodeValueByType or .DecodeValueByColumn instead")
	}
	return spc.rc.DecodeValueByColumn(spc.linkedColumnName, v)
}
