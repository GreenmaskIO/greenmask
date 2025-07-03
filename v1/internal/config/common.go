package config

const (
	defaultTmpDirectoryPath = "/tmp"
)

type Common struct {
	BinPath       string `mapstructure:"bin_path" yaml:"bin_path,omitempty" json:"bin_path,omitempty"`
	TempDirectory string `mapstructure:"tmp_dir" yaml:"tmp_dir,omitempty" json:"tmp_dir,omitempty"`
}

func NewCommon() Common {
	return Common{
		TempDirectory: defaultTmpDirectoryPath,
	}
}
