package caster_tmp

import (
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	DateClass = &TypeClass{
		Name:  "Date",
		Types: []string{"date", "timestamp", "timestamptz"},
	}

	NumericClass = &TypeClass{
		Name:  "NumericClass",
		Types: []string{"numeric", "int2", "int4", "int8", "float4", "float8"},
	}

	IntegerClass = &TypeClass{
		Name:  "IntegerClass",
		Types: []string{"numeric", "int2", "int4", "int8"},
	}

	FloatClass = &TypeClass{
		Name:  "IntegerClass",
		Types: []string{"numeric", "float4", "float8"},
	}

	TextualClass = &TypeClass{
		Name:  "TextualClass",
		Types: []string{"varchar", "text", "bpchar"},
	}

	Classes = []*TypeClass{
		DateClass, NumericClass, IntegerClass, FloatClass, TextualClass,
	}
)

type TypeClass struct {
	Name  string
	Types []string
}

type TypeCastFunc func(driver *toolkit.Driver, input []byte) (output []byte, err error)

func makeCastDecision(driver *toolkit.Driver, inputPgType, outputPgType *pgtype.Type) (
	castFunc TypeCastFunc, warns toolkit.ValidationWarnings, err error,
) {
	// 1. Determine inputType and outputClasses Class
	// 2. Check that type cast can be determined accurately via dynamic casting map (ints -> dates | dates -> ints, etc.)
	//	2.1 Determine via Class first if exists
	//	2.2 Use common type if type does not have a Class
	// 3. If type can be determined via dynamic casting map, then return dynamic functions. Dynamic function must be
	//		able to validate the value and make a strong decision for which domain it belongs. For instance cast
	//	    int Unix (sec, ms, ml, ns) to Date
	// 4. If type does not have a dynamic function then try to find	any function that has intersection with inp and out
	//    types
	// 5. Return the found cast function

}
