package generators

type Settings struct {
	Type       string
	ByteLength int
}

type Generator interface {
	Settings() *Settings
	Generate([]byte) []byte
}
