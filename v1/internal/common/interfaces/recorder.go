package interfaces

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

type Recorder interface {
	SetRow(rawRecord [][]byte) error
	GetRow() [][]byte
	IsNullByColumnName(columName string) (bool, error)
	IsNullByColumnIdx(columIdx int) (bool, error)
	ScanColumnValueByIdx(idx int, v any) (bool, error)
	ScanColumnValueByName(name string, v any) (bool, error)
	GetRawColumnValueByIdx(columnIdx int) (*models.ColumnRawValue, error)
	GetColumnValueByIdx(columnIdx int) (*models.ColumnValue, error)
	GetColumnValueByName(columnName string) (*models.ColumnValue, error)
	GetRawColumnValueByName(columnName string) (*models.ColumnRawValue, error)
	SetColumnValueByIdx(columnIdx int, v any) error
	SetRawColumnValueByIdx(columnIdx int, value *models.ColumnRawValue) error
	SetColumnValueByName(columnName string, v any) error
	SetRawColumnValueByName(columnName string, value *models.ColumnRawValue) error
	GetColumnByName(columnName string) (*models.Column, error)
	TableDriver() TableDriver
}
