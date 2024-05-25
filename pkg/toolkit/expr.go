// An expression handler for the toolkit package. It is used to evaluate the when condition of the record.
// Might be used in transformation conditions and other places where the record is used.

package toolkit

import (
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog/log"
)

const (
	recordExprNamespace    = "record"
	rawRecordExprNamespace = "raw_record"
)

// WhenCond - A condition that should be evaluated to determine if the record should be processed.
type WhenCond struct {
	rc       *RecordContext
	whenCond *vm.Program
	when     string
	env      map[string]any
}

// NewWhenCond - creates a new WhenCond object. It compiles the when condition and returns the compiled program
// and the record context with the functions for the columns. The functions represent the column names and return the
// column values. If the when condition is empty, the WhenCond object will always return true.
func NewWhenCond(when string, driver *Driver, meta map[string]any) (*WhenCond, ValidationWarnings) {
	var (
		rc       *RecordContext
		whenCond *vm.Program
		warnings ValidationWarnings
	)
	if when != "" {
		whenCond, rc, warnings = compileCond(when, driver, meta)
		if warnings.IsFatal() {
			return nil, warnings
		}
	}
	env := FuncMap()
	env["null"] = NullValue
	return &WhenCond{
		rc:       rc,
		whenCond: whenCond,
		when:     when,
		env:      env,
	}, nil
}

// Evaluate - evaluates the when condition. If the when condition is empty, it will always return true.
func (wc *WhenCond) Evaluate(r *Record) (bool, error) {
	if wc.whenCond == nil {
		return true, nil
	}
	wc.rc.SetRecord(r)

	output, err := expr.Run(wc.whenCond, wc.env)
	if err != nil {
		return false, fmt.Errorf("unable to evaluate when condition: %w", err)
	}

	cond, ok := output.(bool)
	if ok {
		return cond, nil
	}

	return false, fmt.Errorf("when condition should return boolean, got (%T) and value %+v", cond, cond)
}

// compileCond compiles the when condition and returns the compiled program and the record context
// with the functions for the columns. The functions represent the column names and return the column values.
// meta - additional meta information for debugging the compilation process
func compileCond(whenCond string, driver *Driver, meta map[string]any) (
	*vm.Program, *RecordContext, ValidationWarnings,
) {
	if whenCond == "" {
		return nil, nil, nil
	}
	scope := "table"
	if _, ok := meta["Transformer"]; ok {
		scope = "transformer"
	}
	meta["Scope"] = scope
	log.Debug().
		Str("WhenCond", whenCond).
		Any("Meta", meta).
		Msg("found when condition: compiling")
	rc, ops := newRecordContext(driver)
	ops = append(ops, expr.Patch(newExprPatcher(meta)))

	cond, err := expr.Compile(whenCond, ops...)
	if err != nil {
		return nil, nil, ValidationWarnings{
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				AddMeta("Error", err.Error()).
				SetMsg("unable to compile when condition"),
		}
	}

	return cond, rc, nil
}

// newRecordContext creates a new record context and create kind of column descriptors for the record to access the
// column values by the column name. For instance if the column name is "name", the function __name will return
// the value
func newRecordContext(driver *Driver) (*RecordContext, []expr.Option) {
	var funcs []expr.Option
	rctx := NewRecordContext()
	for _, c := range driver.Table.Columns {

		// create a function that returns the column value by the column name. The returned value is encoded using
		// pgx driver
		typedFunc := expr.Function(
			fmt.Sprintf("__%s", c.Name),
			func(name string) func(params ...any) (any, error) {
				return func(params ...any) (any, error) {
					v, err := rctx.GetColumnValue(name)
					if err != nil {
						return nil, err
					}
					// convert the value to the appropriate type for expr library
					// the expected types must be nil, bool, int, uint, float32, string, array, map
					switch vv := v.(type) {
					case float32:
						return float64(vv), nil
					case int64:
						return int(vv), nil
					case int32:
						return int(vv), nil
					case int16:
						return int(vv), nil
					case int8:
						return int(vv), nil
					case byte:
						return int(vv), nil
					case uint64:
						return uint(vv), nil
					case uint32:
						return uint(vv), nil
					case uint16:
						return uint(vv), nil
					}
					return v, nil
				}
			}(c.Name),
		)
		funcs = append(funcs, typedFunc)

		rawFunc := expr.Function(
			fmt.Sprintf("__raw__%s", c.Name),
			func(name string) func(params ...any) (any, error) {
				return func(params ...any) (any, error) {
					return rctx.GetColumnRawValue(name)
				}
			}(c.Name),
		)
		funcs = append(funcs, rawFunc)
	}
	return rctx, funcs
}

// exprPatcher - patcher for the expression compiler. It patches the expression tree by some identifiers to
// function calls. For instance is null, is not null, records address
type exprPatcher struct {
	meta map[string]any
}

func newExprPatcher(meta map[string]any) *exprPatcher {
	return &exprPatcher{
		meta: meta,
	}
}

func (ep *exprPatcher) Visit(node *ast.Node) {
	log.Debug().
		Any("Meta", ep.meta).
		Any("Node", node).
		Type("NodeType", *node).
		Str("NodeFmt", fmt.Sprintf("%+v", *node)).
		Msg("debugging expr tree nodes")
	if isRecordOp(node) {
		patchRecordOp(node)
	}
}

// isRecordOp checks if the node is a record operation
func isRecordOp(node *ast.Node) bool {
	mn, ok := (*node).(*ast.MemberNode)
	if !ok {
		return false
	}
	owner, ok := (mn.Node).(*ast.IdentifierNode)
	if !ok {
		return false
	}
	_, ok = (mn.Property).(*ast.StringNode)
	if !ok {
		return false
	}
	return owner.Value == recordExprNamespace || owner.Value == rawRecordExprNamespace
}

// patchRecordOp patches the record access operation
// 1. record.id -> __id() function call for decoding the column value into type using pgx driver
// 2. raw_record.id -> __raw_id() function call getting a raw value as a string
func patchRecordOp(node *ast.Node) {
	mn, ok := (*node).(*ast.MemberNode)
	if !ok {
		return
	}
	owner, ok := (mn.Node).(*ast.IdentifierNode)
	if !ok {
		return
	}
	attr, ok := (mn.Property).(*ast.StringNode)
	if !ok {
		return
	}
	var newOp *ast.CallNode
	switch owner.Value {
	case recordExprNamespace:
		newOp = &ast.CallNode{
			Callee: &ast.IdentifierNode{
				Value: fmt.Sprintf("__%s", attr.Value),
			},
		}

	case rawRecordExprNamespace:
		newOp = &ast.CallNode{
			Callee: &ast.IdentifierNode{
				Value: fmt.Sprintf("__raw__%s", attr.Value),
			},
		}
	}

	log.Debug().Any("OriginalNode", node).Any("NewNode", newOp).Msg("patching record operation")
	ast.Patch(node, newOp)

}
