package config

type StorageDirectory struct {
	Path string `mapstructure:"path"`
}

func NewStorageDirectory() *StorageDirectory {
	return &StorageDirectory{}
}
