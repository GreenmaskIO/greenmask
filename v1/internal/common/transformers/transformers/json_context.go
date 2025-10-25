package transformers

import (
	"github.com/tidwall/gjson"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
)

type JsonContext struct {
	exists        bool
	originalValue any
	path          string
	rc            *template.RecordContextReadOnly
}

func NewJsonContext() *JsonContext {
	return &JsonContext{
		rc: template.NewRecordContextReadOnly(),
	}
}

func (jc *JsonContext) setValue(data []byte, path string) {
	res := gjson.GetBytes(data, path)
	jc.originalValue = res.Value()
	jc.exists = res.Exists()
	jc.path = path
}

func (jc *JsonContext) setRecord(r interfaces.Recorder) {
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
	return jc.rc.GetColumnValue(name)
}

func (jc *JsonContext) GetRawColumnValue(name string) (any, error) {
	return jc.rc.GetRawColumnValue(name)
}

func (jc *JsonContext) EncodeValueByColumn(name string, v any) (any, error) {
	return jc.rc.EncodeValueByColumn(name, v)
}

func (jc *JsonContext) DecodeValueByColumn(name string, v any) (any, error) {
	return jc.rc.DecodeValueByColumn(name, v)
}

func (jc *JsonContext) EncodeValueByType(name string, v any) (any, error) {
	return jc.rc.EncodeValueByType(name, v)
}

func (jc *JsonContext) DecodeValueByType(name string, v any) (any, error) {
	return jc.rc.DecodeValueByType(name, v)
}
