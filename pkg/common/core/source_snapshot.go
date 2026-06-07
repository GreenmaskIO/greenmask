package core

import (
	"fmt"
	"strings"
)

type EntityKind string

type StableKey string

type StableIdentity interface {
	StableKey() (StableKey, error)
}

type DumpContextSnapshot struct {
	Key     StableKey                    `json:"key"`
	Source  SourceSnapshot               `json:"source"`
	Objects map[StableKey]ObjectSnapshot `json:"objects"`
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
	Key        StableKey           `json:"key"`
	Name       AttributeName       `json:"name"`
	Definition AttributeDefinition `json:"definition"`
}

type ObjectSnapshot struct {
	Key      StableKey      `json:"key"`
	Identity EntityIdentity `json:"identity"`

	NeedSchemaDump bool `json:"need_schema_dump"`
	NeedDumpData   bool `json:"need_dump_data"`

	SubsetQuery     string `json:"subset_query,omitempty"`
	SubsetQueryHash string `json:"subset_query_hash,omitempty"`

	Attributes     map[StableKey]ObjectAttribute `json:"attributes,omitempty"`
	AttributesHash string                        `json:"attributes_hash,omitempty"`

	// Object-level condition applied before processing object data.
	Condition *TransformationConditionSnapshot `json:"condition,omitempty"`

	Transformations map[StableKey]TransformationSnapshot `json:"transformations,omitempty"`

	Source SnapshotSource `json:"source,omitempty"`
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
	Key                   StableKey                        `json:"key"`
	Name                  string                           `json:"name"`
	Field                 ObjectFieldRef                   `json:"field"`
	Position              int                              `json:"position"`
	Source                TransformationSource             `json:"source"`
	ConfigHash            string                           `json:"config_hash,omitempty"`
	Config                map[string]any                   `json:"config,omitempty"`
	StaticParametersHash  string                           `json:"static_parameters_hash,omitempty"`
	StaticParameters      map[string]any                   `json:"static_parameters,omitempty"`
	DynamicParametersHash string                           `json:"dynamic_parameters_hash,omitempty"`
	DynamicParameters     map[string]any                   `json:"dynamic_parameters,omitempty"`
	Condition             *TransformationConditionSnapshot `json:"condition,omitempty"`
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

type SnapshotSource struct {
	Kind   SnapshotSourceKind `json:"kind,omitempty"`
	Reason string             `json:"reason,omitempty"`
}

type SnapshotSourceKind string

const (
	SnapshotSourceKindExplicit SnapshotSourceKind = "explicit"
	SnapshotSourceKindDerived  SnapshotSourceKind = "derived"
	SnapshotSourceKindPolicy   SnapshotSourceKind = "policy"
	SnapshotSourceKindDrift    SnapshotSourceKind = "drift"
)
