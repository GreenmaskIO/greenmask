package config

type TablesDataRestorationErrorExclusions struct {
	Name        string   `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Schema      string   `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type GlobalDataRestorationErrorExclusions struct {
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type DataRestorationErrorExclusions struct {
	Tables []TablesDataRestorationErrorExclusions `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Global GlobalDataRestorationErrorExclusions   `mapstructure:"global" yaml:"global" json:"global,omitempty"`
}

type Script struct {
	Name      string   `mapstructure:"name"`
	When      string   `mapstructure:"when"`
	Query     string   `mapstructure:"query"`
	QueryFile string   `mapstructure:"query_file"`
	Command   []string `mapstructure:"command"`
}

type Restore struct {
	RestoreOptions  options                        `mapstructure:"options" yaml:"options" json:"options"`
	Scripts         map[string][]Script            `mapstructure:"scripts" yaml:"scripts" json:"scripts,omitempty"`
	ErrorExclusions DataRestorationErrorExclusions `mapstructure:"insert_error_exclusions" yaml:"insert_error_exclusions" json:"insert_error_exclusions,omitempty"`
}

func NewRestore(o options) Restore {
	return Restore{
		RestoreOptions: o,
	}
}
