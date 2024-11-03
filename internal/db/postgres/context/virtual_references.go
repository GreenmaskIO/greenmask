package context

import (
	"slices"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func getReferencedKeys(r *domains.Reference) (res []string) {
	for _, ref := range r.Columns {
		if ref.Name != "" {
			res = append(res, ref.Name)
		} else if ref.Expression != "" {
			res = append(res, ref.Expression)
		}
	}
	return
}

func validateVirtualReferences(vrs []*domains.VirtualReference, tables []*entries.Table) (res toolkit.ValidationWarnings) {
	for idx, vr := range vrs {
		res = append(res, validateVirtualReference(idx, vr, tables)...)
	}
	return
}

func validateVirtualReference(tableIdx int, vr *domains.VirtualReference, tables []*entries.Table) (res toolkit.ValidationWarnings) {
	if vr.Schema == "" {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("schema is required").
			AddMeta("TableIdx", tableIdx)
		res = append(res, w)
	}
	if vr.Name == "" {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("table columnName is required").
			AddMeta("TableIdx", tableIdx)
		res = append(res, w)
	}
	if len(vr.References) == 0 {
		w := toolkit.NewValidationWarning().
			SetMsg("virtual reference error: references are required: received empty").
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("TableIdx", tableIdx).
			AddMeta("TableName", vr.Name).
			AddMeta("TableSchema", vr.Name)
		res = append(res, w)
	}

	referencedTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
		return t.Name == vr.Name && t.Schema == vr.Schema
	})

	if referencedTableIdx == -1 {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: table not found").
			AddMeta("TableIdx", tableIdx).
			AddMeta("TableName", vr.Name).
			AddMeta("TableSchema", vr.Schema)
		res = append(res, w)
		return
	}

	fkT := tables[referencedTableIdx]

	for idx, v := range vr.References {
		var vrWarns toolkit.ValidationWarnings

		primaryKeyTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
			return t.Name == v.Name && t.Schema == v.Schema
		})
		if primaryKeyTableIdx == -1 {
			w := toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("virtual reference error: table not found").
				AddMeta("ReferenceIdx", idx).
				AddMeta("ReferenceName", v.Name).
				AddMeta("ReferenceSchema", v.Schema)
			res = append(res, w)
			continue
		}
		pkT := tables[primaryKeyTableIdx]

		for _, w := range validateReference(idx, v, fkT, pkT) {
			w.AddMeta("TableIdx", tableIdx).
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("TableName", vr.Name).
				AddMeta("TableSchema", vr.Schema)
			vrWarns = append(vrWarns, w)
		}
		res = append(res, vrWarns...)
	}
	return res
}

func validateReference(vrIdx int, v *domains.Reference, fkT, pkT *entries.Table) (res toolkit.ValidationWarnings) {
	if v.Schema == "" {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: schema is required").
			AddMeta("ReferenceIdx", vrIdx)
		res = append(res, w)
	}
	if v.Name == "" {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: table name is required").
			AddMeta("ReferenceIdx", vrIdx)
		res = append(res, w)
	}
	if len(v.Columns) == 0 {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("columns are required: received empty").
			AddMeta("ReferenceIdx", vrIdx).
			AddMeta("ReferenceName", v.Name).
			AddMeta("ReferenceSchema", v.Schema)
		res = append(res, w)
	}
	refCols := getReferencedKeys(v)
	if len(refCols) != len(pkT.PrimaryKey) {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: number of columns in reference does not match primary key").
			AddMeta("ReferenceIdx", vrIdx).
			AddMeta("ReferencedTableColumns", refCols).
			AddMeta("PrimaryTableColumns", pkT.PrimaryKey).
			AddMeta("ReferenceName", v.Name).
			AddMeta("ReferenceSchema", v.Schema)
		res = append(res, w)
	}

	if len(v.PolymorphicExprs) > 0 && v.NotNull {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: polymorphic expressions cannot be used with not null reference").
			AddMeta("ReferenceIdx", vrIdx).
			AddMeta("ReferenceName", v.Name).
			AddMeta("ReferenceSchema", v.Schema)
		res = append(res, w)
	}

	for idx, c := range v.Columns {
		var vrWarns toolkit.ValidationWarnings
		for _, w := range validateColumn(idx, c, fkT) {
			w.AddMeta("ReferenceIdx", vrIdx).
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ReferenceName", v.Name).
				AddMeta("ReferenceSchema", v.Schema)
			vrWarns = append(vrWarns, w)
		}
		res = append(res, vrWarns...)
	}

	return res
}

func validateColumn(colIdx int, c *domains.ReferencedColumn, fkT *entries.Table) (res toolkit.ValidationWarnings) {
	if c.Name == "" && c.Expression == "" {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: name or expression is required").
			AddMeta("ColumnIdx", colIdx)
		res = append(res, w)
	}
	if c.Name != "" && c.Expression != "" {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: name and expression are mutually exclusive").
			AddMeta("ColumnIdx", colIdx)
		res = append(res, w)
	}
	if c.Name != "" && !slices.ContainsFunc(fkT.Columns, func(column *toolkit.Column) bool {
		return column.Name == c.Name
	}) {
		w := toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("virtual reference error: column not found").
			AddMeta("ColumnIdx", colIdx).
			AddMeta("ColumnName", c.Name)
		res = append(res, w)
	}

	return res
}
