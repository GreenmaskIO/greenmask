package core

import (
	"fmt"
	"strings"
)

type EntityKind string

const (
	EntityKindMysqlTable  EntityKind = "mysql.table"
	EntityKindMysqlServer EntityKind = "mysql.server"
	EntityKindMysqlColumn EntityKind = "mysql.column"
)

type StableKey string

type StableIdentity interface {
	StableKey() (StableKey, error)
}

// SnapshotSchemaVersionV1 identifies the schema of DumpContextSnapshot.
// Increment this constant whenever a field is added that the differ must treat
// differently from an absent field (i.e. "not captured" vs "empty").
//
// Schema evolution rules:
//   - Adding optional (omitempty) fields: safe; no bump needed unless the differ
//     must distinguish absence from empty.
//   - Renaming a JSON key: BREAKING — bump the version and add a migration note.
//   - Changing a field type: BREAKING — bump the version and add a migration note.
const SnapshotSchemaVersionV1 = "1"

type DumpContextSnapshot struct {
	// SchemaVersion records the snapshot schema at creation time.
	// Empty string means the snapshot predates versioning (treat as "0").
	SchemaVersion string                       `json:"schema_version,omitempty"`
	Key           StableKey                    `json:"key"`
	Source        SourceSnapshot               `json:"source"`
	Objects       map[StableKey]ObjectSnapshot `json:"objects"`

	// Meta carries informational, run-specific values (e.g. server version,
	// gtid/binlog position, snapshot id). It is EXCLUDED from every hash,
	// fingerprint, and drift diff: these values change every run and would
	// otherwise produce false-positive drift.
	Meta map[string]string `json:"meta,omitempty"`
}

type SourceSnapshot struct {
	Identity    EntityIdentity `json:"identity"`
	FilterHash  string         `json:"filter_hash,omitempty"`
	Filters     map[string]any `json:"filters,omitempty"`
	DBMSVersion string         `json:"dbms_version,omitempty"`
	// Vendor-specific source parameters.
	//
	// Examples:
	//   - PostgreSQL:
	//       snapshot_id
	//       tx_isolation
	//   - MySQL:
	//       binlog_file
	//       gtid
	//   - Oracle:
	//       scn
	VendorParameters     map[string]any `json:"vendor_parameters,omitempty"`
	VendorParametersHash string         `json:"vendor_parameters_hash,omitempty"`
}

type AttributeName string

type AttributeDefinition string

type ObjectAttribute struct {
	// Identity is the stable identity of the attribute, relative to its parent
	// object (e.g. kind=column, name=column name). Consistent with how objects
	// and the source identify themselves.
	Identity EntityIdentity `json:"identity"`
	// Position is the attribute's ordinal position in the object (column order).
	Position   int                 `json:"position"`
	Definition AttributeDefinition `json:"definition"`
}

// ColumnAttributeIdentity builds the relative identity of a column attribute.
// kind is the engine-specific column kind (e.g. EntityKindMysqlColumn); the
// attribute is scoped within its parent object so only the column name is
// carried.
func ColumnAttributeIdentity(kind EntityKind, name string) EntityIdentity {
	return EntityIdentity{
		Kind:       kind,
		NameParts:  []string{"column"},
		NameValues: map[string]string{"column": name},
	}
}

// ObjectSnapshot captures a single data object. Schema-dump intent is not
// recorded here: it is driven separately (vendor tools, scope matching) and is
// not reliably correlatable across runs, so the snapshot compares data objects
// only.
type ObjectSnapshot struct {
	Key      StableKey      `json:"key"`
	Identity EntityIdentity `json:"identity"`

	SubsetQuery     string `json:"subset_query,omitempty"`
	SubsetQueryHash string `json:"subset_query_hash,omitempty"`

	Attributes     map[StableKey]ObjectAttribute `json:"attributes,omitempty"`
	AttributesHash string                        `json:"attributes_hash,omitempty"`

	// Object-level condition applied before processing object data.
	Condition *TransformationConditionSnapshot `json:"condition,omitempty"`

	Transformations map[StableKey]TransformationSnapshot `json:"transformations,omitempty"`

	// Origin records whether the object was set by the explicit or derived
	// context builder.
	Origin ObjectOrigin `json:"origin,omitempty"`
}

type EntityIdentity struct {
	Kind EntityKind `json:"kind"`

	// Ordered logical scope keys.
	//
	// Examples:
	//   - postgres.table: ["database", "schema", "table"]
	//   - mysql.table: ["database", "table"]
	NameParts []string `json:"name_parts"`

	// Scope values by logical key.
	NameValues map[string]string `json:"name_values,omitempty"`
}

func (i EntityIdentity) Name() (string, error) {
	parts := make([]string, 0, len(i.NameParts))

	for _, key := range i.NameParts {
		value, ok := i.NameValues[key]
		if !ok {
			return "", fmt.Errorf("missing name value for key %q", key)
		}
		if value == "" {
			return "", fmt.Errorf("empty name value for key %q", key)
		}

		parts = append(parts, value)
	}

	if len(parts) == 0 {
		return "", fmt.Errorf("object identity has no name parts")
	}

	return strings.Join(parts, "."), nil
}

func (i EntityIdentity) StableKey() (StableKey, error) {
	name, err := i.Name()
	if err != nil {
		return "", err
	}

	return StableKey(fmt.Sprintf("%s:%s", i.Kind, name)), nil
}

type TransformationSnapshot struct {
	// Stable matching key: where transformation is applied.
	Key                   StableKey            `json:"key"`
	Name                  string               `json:"name"`
	Field                 ObjectFieldRef       `json:"field"`
	Position              int                  `json:"position"`
	Source                TransformationSource `json:"source"`
	ConfigHash            string               `json:"config_hash,omitempty"`
	Config                map[string]any       `json:"config,omitempty"`
	StaticParametersHash  string               `json:"static_parameters_hash,omitempty"`
	StaticParameters      map[string]any       `json:"static_parameters,omitempty"`
	DynamicParametersHash string               `json:"dynamic_parameters_hash,omitempty"`
	DynamicParameters     map[string]any       `json:"dynamic_parameters,omitempty"`
	// AffectedColumns are the columns the transformer writes to, ordered by
	// column index for determinism.
	AffectedColumns     []string `json:"affected_columns,omitempty"`
	AffectedColumnsHash string   `json:"affected_columns_hash,omitempty"`
	// Condition is the transformer-level when expression (empty when none).
	Condition string `json:"condition,omitempty"`
	// Full semantic fingerprint.
	Fingerprint string `json:"fingerprint,omitempty"`
}

type TransformationConditionKind string

const (
	TransformationConditionKindExpression TransformationConditionKind = "expression"
	TransformationConditionKindAlways     TransformationConditionKind = "always"
	TransformationConditionKindNever      TransformationConditionKind = "never"
)

type TransformationConditionSnapshot struct {
	Kind TransformationConditionKind `json:"kind"`

	// Stable normalized condition expression.
	Expression string `json:"expression,omitempty"`

	Parameters map[string]any `json:"parameters,omitempty"`

	// Fingerprint is calculated from Kind + normalized Expression + Parameters.
	Fingerprint string `json:"fingerprint,omitempty"`
}

func (t TransformationSnapshot) StableKey() (StableKey, error) {
	return StableKey(
		fmt.Sprintf("%s:%s:%d:%s", t.Field.Kind, t.Field.Value, t.Position, t.Name),
	), nil
}

// NewConditionSnapshot builds a condition snapshot from a normalized expression,
// computing its fingerprint. Returns nil for an empty expression (no condition).
func NewConditionSnapshot(expression string) *TransformationConditionSnapshot {
	if expression == "" {
		return nil
	}
	cs := &TransformationConditionSnapshot{
		Kind:       TransformationConditionKindExpression,
		Expression: expression,
	}
	cs.Fingerprint = HashStrings([]string{string(cs.Kind), cs.Expression})
	return cs
}

type TransformationSourceKind string

const (
	TransformationSourceKindExplicit TransformationSourceKind = "explicit"
	TransformationSourceKindDerived  TransformationSourceKind = "derived"
	TransformationSourceKindPolicy   TransformationSourceKind = "policy"
	TransformationSourceKindAuto     TransformationSourceKind = "auto"
)

type TransformationSource struct {
	Kind TransformationSourceKind `json:"kind"`

	// Optional explanation/debug fields.
	Reason string `json:"reason,omitempty"`

	// For inherited/derived transformations.
	DerivedFrom *TransformationDerivationRef `json:"derived_from,omitempty"`
}

type TransformationDerivationRef struct {
	Object   EntityIdentity `json:"object"`
	Field    ObjectFieldRef `json:"field"`
	LinkKind ObjectLinkKind `json:"link_kind,omitempty"`
}

// ObjectOrigin records which context builder put an object into the dump:
// explicit (user configuration) or derived (semantic derivation, e.g. a
// primary-key transformation inherited by a referencing foreign-key column).
type ObjectOrigin struct {
	Kind ObjectOriginKind `json:"kind,omitempty"`
	// Reason explains a derived origin (e.g. "inherited primary-key transformation").
	Reason string `json:"reason,omitempty"`
}

type ObjectOriginKind string

const (
	ObjectOriginExplicit ObjectOriginKind = "explicit"
	ObjectOriginDerived  ObjectOriginKind = "derived"
)
