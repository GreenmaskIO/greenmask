package models

type Object struct {
	ID   ObjectID
	Kind ObjectKind
	Name string
	// Engine specific payload.
	// e.g. *postgres.Table, *oracle.Package, *mongo.Collection
	Payload any
}
