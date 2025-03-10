package config

type Validate struct {
	Tables           []string `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Data             bool     `mapstructure:"data" yaml:"data" json:"data,omitempty"`
	Diff             bool     `mapstructure:"diff" yaml:"diff" json:"diff,omitempty"`
	Schema           bool     `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	RowsLimit        uint64   `mapstructure:"rows_limit" yaml:"rows_limit" json:"rows_limit,omitempty"`
	ResolvedWarnings []string `mapstructure:"resolved_warnings" yaml:"resolved_warnings" json:"resolved_warnings,omitempty"`
	TableFormat      string   `mapstructure:"table_format" yaml:"table_format" json:"table_format,omitempty"`
	Format           string   `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	OnlyTransformed  bool     `mapstructure:"transformed_only" yaml:"transformed_only" json:"transformed_only,omitempty"`
	Warnings         bool     `mapstructure:"warnings" yaml:"warnings" json:"warnings,omitempty"`
}
