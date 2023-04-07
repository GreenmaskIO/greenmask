package toc

const (
	PgCompressionNone int32 = iota
	PgCompressionGzip
	PgCompressionLz4
	PgCompressionZSTD
)

type CompressionSpecification struct {
	Algorithm int32
	Options   uint32
	Level     int32
	Workers   int32
}
