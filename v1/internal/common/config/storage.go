package config

const (
	defaultStorageType = "directory"
)

type Storage struct {
	Type      string            `mapstructure:"type" yaml:"type" json:"type,omitempty"`
	S3        *StorageS3        `mapstructure:"s3"  json:"s3,omitempty" yaml:"s3"`
	Directory *StorageDirectory `mapstructure:"directory" json:"directory,omitempty" yaml:"directory"`
}

func NewStorage() Storage {
	return Storage{
		Type:      defaultStorageType,
		S3:        NewStorageS3(),
		Directory: NewStorageDirectory(),
	}
}
