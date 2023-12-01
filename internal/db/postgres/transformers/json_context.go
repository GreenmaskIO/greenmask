package transformers

import (
	"github.com/tidwall/gjson"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils/template"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type JsonContext struct {
	exists        bool
	originalValue any
	path          string
	rc            *template.RecordContext
}

func NewJsonContext() *JsonContext {
	return &JsonContext{
		rc: &template.RecordContext{},
	}
}

func (jc *JsonContext) setValue(data []byte, path string) {
	res := gjson.GetBytes(data, path)
	jc.originalValue = res.Value()
	jc.exists = res.Exists()
	jc.path = path
}

func (jc *JsonContext) setRecord(r *toolkit.Record) {
	jc.rc.SetRecord(r)
}

func (jc *JsonContext) GetPath() string {
	return jc.path
}

func (jc *JsonContext) GetOriginalValue() any {
	return jc.originalValue
}

func (jc *JsonContext) OriginalValueExists() bool {
	return jc.exists
}

func (jc *JsonContext) GetColumnValue(name string) (any, error) {
	return jc.rc.GetValue(name)
}

func (jc *JsonContext) GetColumnRawValue(name string) (any, error) {
	return jc.rc.GetRawValue(name)
}

func (jc *JsonContext) EncodeValueByColumn(name string, v any) (any, error) {
	return jc.rc.EncodeValue(name, v)
}

func (jc *JsonContext) DecodeValueByColumn(name string, v any) (any, error) {
	return jc.rc.DecodeValue(name, v)
}

func (jc *JsonContext) EncodeValueByType(name string, v any) (any, error) {
	return jc.rc.EncodeValueByType(name, v)
}

func (jc *JsonContext) DecodeValueByType(name string, v any) (any, error) {
	return jc.rc.DecodeValueByType(name, v)
}
