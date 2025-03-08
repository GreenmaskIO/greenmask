package storages

type DirectoryConfig struct {
	Path string `mapstructure:"path"`
}

func NewStorageDirectory() *DirectoryConfig {
	return &DirectoryConfig{}
}
