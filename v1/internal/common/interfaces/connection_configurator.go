package interfaces

type ConnectionConfigurator interface {
	URI() (string, error)
}
