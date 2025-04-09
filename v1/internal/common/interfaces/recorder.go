package interfaces

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

type Recorder interface {
	GetRawColumnValueByIdx(columnIdx int) (*models.ColumnRawValue, error)
	GetColumnValueByIdx(columnIdx int) (*models.ColumnValue, error)
	GetColumnValueByName(columnName string) (*models.ColumnValue, error)
	GetRawColumnValueByName(columnName string) (*models.ColumnRawValue, error)
	SetColumnValueByIdx(columnIdx int, v any) error
	SetRawColumnValueByIdx(columnIdx int, value *models.ColumnRawValue) error
	SetColumnValueByName(columnName string, v any) error
	SetRawColumnValueByName(columnName string, value *models.ColumnRawValue) error
	GetColumnByName(columnName string) (*models.Column, bool)
	TableDriver() TableDriver
}
