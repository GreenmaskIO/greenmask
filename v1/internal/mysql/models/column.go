package models

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

type Column struct {
	Idx               int
	Name              string
	TypeName          string
	DataType          *string
	NumericPrecision  *int
	NumericScale      *int
	DateTimePrecision *int
	NotNull           bool
	TypeOID           models.VirtualOID
}

func NewColumn(
	idx int,
	name, typeName string,
	dataType *string,
	numericPrecision, numericScale, dateTimePrecision *int,
	notNull bool,
	typeOID models.VirtualOID,
) Column {
	return Column{
		Idx:               idx,
		Name:              name,
		TypeName:          typeName,
		DataType:          dataType,
		NumericPrecision:  numericPrecision,
		NumericScale:      numericScale,
		DateTimePrecision: dateTimePrecision,
		NotNull:           notNull,
		TypeOID:           typeOID,
	}
}
