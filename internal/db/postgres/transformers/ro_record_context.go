package transformers

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type RoRecordContext struct {
	rc *toolkit.RecordContext
}

func NewRoRecordContext() *RoRecordContext {
	return &RoRecordContext{
		rc: &toolkit.RecordContext{},
	}
}

func (cc *RoRecordContext) clean() {
	cc.rc.Clean()
}

func (cc *RoRecordContext) setRecord(r *toolkit.Record) {
	cc.rc.SetRecord(r)
}

func (cc *RoRecordContext) GetColumnValue(name string) (any, error) {
	return cc.rc.GetColumnValue(name)
}

func (cc *RoRecordContext) GetColumnRawValue(name string) (any, error) {
	return cc.rc.GetRawColumnValue(name)
}

func (cc *RoRecordContext) EncodeValueByColumn(name string, v any) (any, error) {
	return cc.rc.EncodeValueByColumn(name, v)
}

func (cc *RoRecordContext) DecodeValueByColumn(name string, v any) (any, error) {
	return cc.rc.DecodeValueByColumn(name, v)
}

func (cc *RoRecordContext) EncodeValueByType(name string, v any) (any, error) {
	return cc.rc.EncodeValueByType(name, v)
}

func (cc *RoRecordContext) DecodeValueByType(name string, v any) (any, error) {
	return cc.rc.DecodeValueByType(name, v)
}
