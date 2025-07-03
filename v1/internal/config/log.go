package config

type Log struct {
	Format string `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	Level  string `mapstructure:"level" yaml:"level" json:"level,omitempty"`
}
