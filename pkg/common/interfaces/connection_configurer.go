package interfaces

type ConnectionConfigurer interface {
	ConnectionConfig() any
}

// ConnectionConfigurerBuilder translates a DBMS-agnostic config value into
// a DBMS-specific ConnectionConfigurer. Build receives the full config as any
// to avoid an import cycle (pkg/config already imports this package).
// Implementations type-assert to config.Config internally.
type ConnectionConfigurerBuilder interface {
	Build(cfg any) (ConnectionConfigurer, error)
}
