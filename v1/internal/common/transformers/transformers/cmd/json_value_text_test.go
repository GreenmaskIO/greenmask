package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonAttrRawValueText_SetData(t *testing.T) {
	val := NewJsonAttrRawValueText()
	val.SetData([]byte("test"))
	val.SetNull(true)
	assert.True(t, val.IsValueNull())
	assert.Equal(t, []byte("test"), val.GetData())
	assert.Equal(t, "test", *val.Data)
	assert.True(t, val.IsNull)
}
