// An expression handler for the toolkit package. It is used to evaluate when condition of the record.
// Might be used in transformation conditions and other places where the record is used.

package conditions

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const (
	recordExprNamespace    = "record"
	rawRecordExprNamespace = "raw_record"
)

const (
	metaScopeTable       = "table"
	metaTableTransformer = "transformer"
)

// WhenCond - A condition that should be evaluated to determine if the record should be processed.
type WhenCond struct {
	rc       *template.RecordContextReadOnly
	whenCond *vm.Program
	when     string
	env      map[string]any
}

// NewWhenCond - creates a new WhenCond object. It compiles when condition and returns the compiled program
// and the record context with the functions for the columns. The functions represent the column names and return the
// column values. If when condition is empty, the WhenCond object will always return true.
func NewWhenCond(
	ctx context.Context,
	vc *validationcollector.Collector,
	when string,
	table commonmodels.Table,
) (*WhenCond, error) {
	var (
		rc       *template.RecordContextReadOnly
		whenCond *vm.Program
		err      error
	)
	if when != "" {
		whenCond, rc, err = compileCond(ctx, vc, when, table)
		if err != nil {
			return nil, fmt.Errorf("compile condition: %w", err)
		}
	}
	env := template.FuncMap()
	env["null"] = template.NullValue
	return &WhenCond{
		rc:       rc,
		whenCond: whenCond,
		when:     when,
		env:      env,
	}, nil
}

// Evaluate - evaluates when condition. If when condition is empty, it will always return true.
func (wc *WhenCond) Evaluate(r commonininterfaces.Recorder) (bool, error) {
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

// compileCond compiles when condition and returns the compiled program and the record context
// with the functions for the columns. The functions represent the column names and return the column values.
// meta - additional meta information for debugging the compilation process
func compileCond(
	ctx context.Context,
	vc *validationcollector.Collector,
	whenCond string,
	table commonmodels.Table,
) (*vm.Program, *template.RecordContextReadOnly, error) {
	if whenCond == "" {
		return nil, nil, nil
	}
	scope := metaScopeTable
	if _, ok := vc.GetMetaKey("TransformerName"); ok {
		scope = metaTableTransformer
	}
	vc = vc.WithMeta(map[string]any{"Scope": scope})
	log.Ctx(ctx).Debug().
		Str("WhenCond", whenCond).
		Any("Meta", vc.GetMeta()).
		Msg("found when condition: compiling")
	rc, ops := newRecordContext(table)
	ops = append(ops, expr.Patch(newExprPatcher(ctx, vc.GetMeta())))

	cond, err := expr.Compile(whenCond, ops...)
	if err != nil {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			AddMeta("Error", err.Error()).
			SetMsg("unable to compile when condition"))
		return nil, nil, commonmodels.ErrFatalValidationError
	}

	return cond, rc, nil
}

// newRecordContext creates a new record context and create kind of column descriptors for the record to access the
// column values by the column name. For instance if the column name is "name", the function __name will return
// the value
func newRecordContext(table commonmodels.Table) (*template.RecordContextReadOnly, []expr.Option) {
	intSize := unsafe.Sizeof(int(0)) * 8
	var funcs []expr.Option
	rctx := template.NewRecordContextReadOnly()
	for _, c := range table.Columns {

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
						raiseAnErrorIfSysIs32AndDriverReturns64(intSize)
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
					return rctx.GetRawColumnValue(name)
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
	ctx  context.Context
	meta map[string]any
}

func newExprPatcher(ctx context.Context, meta map[string]any) *exprPatcher {
	return &exprPatcher{
		ctx:  ctx,
		meta: meta,
	}
}

func (ep *exprPatcher) Visit(node *ast.Node) {
	log.Ctx(ep.ctx).Debug().
		Any("Meta", ep.meta).
		Any("Node", node).
		Type("NodeType", *node).
		Str("NodeFmt", fmt.Sprintf("%+v", *node)).
		Msg("debugging expr tree nodes")
	if isRecordOp(node) {
		patchRecordOp(ep.ctx, node)
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
func patchRecordOp(ctx context.Context, node *ast.Node) {
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

	log.Ctx(ctx).Debug().
		Any("OriginalNode", node).
		Any("NewNode", newOp).
		Msg("patching record operation")
	ast.Patch(node, newOp)

}

// raiseAnErrorIfSysIs32AndDriverReturns64 - raises an error if the system is 32 bit and the driver returns 64 bit
// values. In 32-bit system int type is 32 bit but int 64 is 64 bit. In this case the int8 postgresql type cannot be
// handled using int type in go because it cast to int32 and loses the data. This is limitation of the go expr library
func raiseAnErrorIfSysIs32AndDriverReturns64(sysBytes uintptr) {
	if sysBytes == 32 {
		panic("go expr and pgx driver are not compatible to handle int8 postgresql type using int type in go")
	}
}
