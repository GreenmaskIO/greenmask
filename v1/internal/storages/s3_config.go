package storages

const (
	defaultS3StorageMaxRetries   = 3
	defaultS3StorageMaxPartSize  = 50 * 1024 * 1024
	defaultS3StorageStorageClass = "STANDARD"
	defaultS3StorageForcePath    = true
)

type S3Config struct {
	Endpoint         string
	Bucket           string
	Prefix           string
	Region           string
	StorageClass     string
	DisableSSL       bool
	AccessKeyId      string
	SecretAccessKey  string
	SessionToken     string
	RoleArn          string
	SessionName      string
	MaxRetries       int
	CertFile         string
	MaxPartSize      int64
	Concurrency      int
	UseListObjectsV1 bool
	ForcePathStyle   bool
	UseAccelerate    bool
	NoVerifySsl      bool
}

func NewS3Config(
	endpoint string,
	bucket string,
	prefix string,
	region string,
	storageClass string,
	accessKeyId string,
	secretAccessKey string,
	sessionToken string,
	roleArn string,
	sessionName string,
	maxRetries int,
	certFile string,
	maxPartSize int64,
	concurrency int,
	useListObjectsV1 bool,
	forcePathStyle *bool,
	useAccelerate bool,
	noVerifySsl bool,
) S3Config {
	if maxRetries == -1 {
		maxRetries = defaultS3StorageMaxRetries
	}
	if maxPartSize == -1 {
		maxPartSize = defaultS3StorageMaxPartSize
	}
	if storageClass == "" {
		storageClass = defaultS3StorageStorageClass
	}
	var forcePathStyleValue bool
	if forcePathStyle == nil {
		forcePathStyleValue = defaultS3StorageForcePath
	} else {
		forcePathStyleValue = *forcePathStyle
	}
	return S3Config{
		Endpoint:         endpoint,
		Bucket:           bucket,
		Prefix:           prefix,
		Region:           region,
		StorageClass:     storageClass,
		AccessKeyId:      accessKeyId,
		SecretAccessKey:  secretAccessKey,
		SessionToken:     sessionToken,
		RoleArn:          roleArn,
		SessionName:      sessionName,
		CertFile:         certFile,
		MaxRetries:       maxRetries,
		MaxPartSize:      maxPartSize,
		Concurrency:      concurrency,
		UseListObjectsV1: useListObjectsV1,
		ForcePathStyle:   forcePathStyleValue,
		UseAccelerate:    useAccelerate,
		NoVerifySsl:      noVerifySsl,
	}
}
