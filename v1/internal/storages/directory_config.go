package storages

type DirectoryConfig struct {
	Path string
}

func NewDirectoryConfig(path string) DirectoryConfig {
	return DirectoryConfig{
		Path: path,
	}
}
