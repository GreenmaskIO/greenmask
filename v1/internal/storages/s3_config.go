package storages

const (
	defaultS3StorageMaxRetries   = 3
	defaultS3StorageMaxPartSize  = 50 * 1024 * 1024
	defaultS3StorageStorageClass = "STANDARD"
	defaultS3StorageForcePath    = true
)

type S3Config struct {
	Endpoint         string `mapstructure:"endpoint"`
	Bucket           string `mapstructure:"bucket"`
	Prefix           string `mapstructure:"prefix"`
	Region           string `mapstructure:"region"`
	StorageClass     string `mapstructure:"storage_class"`
	DisableSSL       bool   `mapstructure:"disable_ssl"`
	AccessKeyId      string `mapstructure:"access_key_id"`
	SecretAccessKey  string `mapstructure:"secret_access_key"`
	SessionToken     string `mapstructure:"session_token"`
	RoleArn          string `mapstructure:"role_arn"`
	SessionName      string `mapstructure:"session_name"`
	MaxRetries       int    `mapstructure:"max_retries"`
	CertFile         string `mapstructure:"cert_file"`
	MaxPartSize      int64  `mapstructure:"max_part_size"`
	Concurrency      int    `mapstructure:"concurrency"`
	UseListObjectsV1 bool   `mapstructure:"use_list_objects_v1"`
	ForcePathStyle   bool   `mapstructure:"force_path_style"`
	UseAccelerate    bool   `mapstructure:"use_accelerate"`
	NoVerifySsl      bool   `mapstructure:"no_verify_ssl"`
}

func NewStorageS3() *S3Config {
	return &S3Config{
		StorageClass:   defaultS3StorageStorageClass,
		ForcePathStyle: defaultS3StorageForcePath,
		MaxRetries:     defaultS3StorageMaxRetries,
		MaxPartSize:    defaultS3StorageMaxPartSize,
	}
}
