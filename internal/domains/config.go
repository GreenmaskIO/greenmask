package domains

type Config struct {
	Connection     string   `yaml:"connection"`
	Engine         string   `yaml:"engine"`
	IncludeSchemas []string `yaml:"include_schemas"`
	ExcludeSchemas []string `yaml:"exclude_schemas"`
	IncludeTables  []string `yaml:"include_tables"`
	ExcludeTables  []string `yaml:"exclude_tables"`
	TableRules     []Table  `yaml:"table_rules"`
}
