package sqlite

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// ContractError allows handlers to return specific error codes and messages.
type ContractError interface {
	error
	Code() int
}

type defaultError struct {
	code    int
	message string
}

func (e *defaultError) Error() string { return e.message }
func (e *defaultError) Code() int     { return e.code }

// NewError creates a new ContractError with the specified code and message.
func NewError(code int, message string) ContractError {
	return &defaultError{code: code, message: message}
}

// ErrNotFound is a sentinel error for record-not-found.
var ErrNotFound = NewError(404, "record not found")

// Op defines the database operation type.
type Op string

const (
	OpCreate Op = "create"
	OpFind   Op = "find"   // single record by conditions
	OpList   Op = "list"   // multiple records
	OpUpdate Op = "update" // update by conditions
	OpDelete Op = "delete" // delete by conditions
	OpExec   Op = "exec"   // raw execution
	OpCount  Op = "count"  // count records
)

// Handler is a generic wrapper that converts a strongly-typed contract method
// into a GORM-backed database operation.
// It inspects the Req struct tags to determine the operation and builds the query.
func Handler[Req any, Resp any](db *gorm.DB, op Op) func(context.Context, *Req) (*Resp, error) {
	return func(ctx context.Context, req *Req) (*Resp, error) {
		tx := db.WithContext(ctx)
		reqType := reflect.TypeOf((*Req)(nil)).Elem()
		respType := reflect.TypeOf((*Resp)(nil)).Elem()

		result, err := dispatch(tx, req, reqType, respType, op)
		if err != nil {
			return nil, err
		}

		if result == nil {
			return nil, nil
		}

		return result.(*Resp), nil
	}
}

// Mount scans a service struct and auto-wires all contract methods to GORM operations.
// Each method must follow the pattern: Method(ctx, *Params) (*Result, error)
// The Params struct must embed or contain an `Op` field tagged with `db:"op"`.
//
// Alternatively, if no Op field exists in Params, the method name prefix is used:
//   - Create*, Insert*  -> OpCreate
//   - Get*, Find*       -> OpFind
//   - List*, Search*    -> OpList
//   - Update*, Save*    -> OpUpdate
//   - Delete*, Remove*  -> OpDelete
//   - Count*            -> OpCount
//   - Exec*, Raw*       -> OpExec
func Mount(db *gorm.DB, service any) error {
	svcType := reflect.TypeOf(service)
	svcVal := reflect.ValueOf(service)

	if svcType.Kind() != reflect.Ptr || svcVal.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("service must be a pointer to a struct")
	}

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	errType := reflect.TypeOf((*error)(nil)).Elem()

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		mType := method.Type

		// Must match: (receiver, context.Context, *Params) (*Result, error)
		if mType.NumIn() != 3 || mType.NumOut() != 2 {
			continue
		}
		if !mType.In(1).Implements(ctxType) {
			continue
		}
		reqType := mType.In(2)
		if reqType.Kind() != reflect.Ptr {
			continue
		}
		if !mType.Out(1).Implements(errType) {
			continue
		}

		// Determine operation from method name
		op := inferOp(method.Name)
		if op == "" {
			continue
		}

		// Create the dynamic implementation and set it on the struct's method field
		implFunc := createDynamicHandler(db, svcVal, method, reqType, op)

		// Try to find and set the corresponding function field on the struct
		fieldName := method.Name + "Fn"
		field := svcVal.Elem().FieldByName(fieldName)
		if field.IsValid() && field.CanSet() && field.Type().Kind() == reflect.Func {
			field.Set(implFunc)
		}
	}

	return nil
}

// Bind creates a repository by scanning the service struct and auto-wiring
// all exported function fields that match the contract pattern.
// The function fields should be named like: CreateUser, FindUser, ListUsers, etc.
// Their type must be: func(context.Context, *Params) (*Result, error)
func Bind(db *gorm.DB, service any) error {
	svcVal := reflect.ValueOf(service)
	if svcVal.Kind() != reflect.Ptr {
		return fmt.Errorf("service must be a pointer to a struct")
	}
	svcVal = svcVal.Elem()
	if svcVal.Kind() != reflect.Struct {
		return fmt.Errorf("service must be a pointer to a struct")
	}

	svcType := svcVal.Type()
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	errType := reflect.TypeOf((*error)(nil)).Elem()

	for i := 0; i < svcType.NumField(); i++ {
		field := svcType.Field(i)
		fieldVal := svcVal.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		fType := field.Type
		if fType.Kind() != reflect.Func {
			continue
		}

		// Validate signature: func(context.Context, *Params) (*Result, error)
		if fType.NumIn() != 2 || fType.NumOut() != 2 {
			continue
		}
		if !fType.In(0).Implements(ctxType) {
			continue
		}
		reqType := fType.In(1)
		if reqType.Kind() != reflect.Ptr {
			continue
		}
		if !fType.Out(1).Implements(errType) {
			continue
		}

		// Determine op from field name
		op := inferOp(field.Name)
		if op == "" {
			continue
		}

		respType := fType.Out(0)

		// Build the dynamic handler
		handler := createFieldHandler(db, reqType, respType, op)
		fieldVal.Set(handler)
	}

	return nil
}

// inferOp determines the database operation from a method/field name prefix.
func inferOp(name string) Op {
	lower := strings.ToLower(name)
	switch {
	case strings.HasPrefix(lower, "create"), strings.HasPrefix(lower, "insert"),
		strings.HasPrefix(lower, "add"), strings.HasPrefix(lower, "new"):
		return OpCreate
	case strings.HasPrefix(lower, "get"), strings.HasPrefix(lower, "find"),
		strings.HasPrefix(lower, "fetch"), strings.HasPrefix(lower, "load"):
		return OpFind
	case strings.HasPrefix(lower, "list"), strings.HasPrefix(lower, "search"),
		strings.HasPrefix(lower, "all"), strings.HasPrefix(lower, "query"):
		return OpList
	case strings.HasPrefix(lower, "update"), strings.HasPrefix(lower, "save"),
		strings.HasPrefix(lower, "modify"), strings.HasPrefix(lower, "patch"):
		return OpUpdate
	case strings.HasPrefix(lower, "delete"), strings.HasPrefix(lower, "remove"),
		strings.HasPrefix(lower, "drop"):
		return OpDelete
	case strings.HasPrefix(lower, "count"):
		return OpCount
	case strings.HasPrefix(lower, "exec"), strings.HasPrefix(lower, "raw"):
		return OpExec
	}
	return ""
}

// createDynamicHandler creates a reflect.Value function for Mount-based wiring.
func createDynamicHandler(db *gorm.DB, svc reflect.Value, method reflect.Method, reqType reflect.Type, op Op) reflect.Value {
	return reflect.MakeFunc(method.Type, func(args []reflect.Value) []reflect.Value {
		ctx := args[1].Interface().(context.Context)
		req := args[2].Interface()
		tx := db.WithContext(ctx)

		result, err := dispatch(tx, req, reqType.Elem(), method.Type.Out(0), op)
		return packResults(method.Type, result, err)
	})
}

// createFieldHandler creates a reflect.Value function for Bind-based wiring.
func createFieldHandler(db *gorm.DB, reqType, respType reflect.Type, op Op) reflect.Value {
	fnType := reflect.FuncOf(
		[]reflect.Type{reflect.TypeOf((*context.Context)(nil)).Elem(), reqType},
		[]reflect.Type{respType, reflect.TypeOf((*error)(nil)).Elem()},
		false,
	)

	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		ctx := args[0].Interface().(context.Context)
		req := args[1].Interface()
		tx := db.WithContext(ctx)

		result, err := dispatch(tx, req, reqType.Elem(), respType, op)
		return packFieldResults(respType, result, err)
	})
}

// packResults packs result + error into reflect.Value slice for method calls.
func packResults(mType reflect.Type, result any, err error) []reflect.Value {
	respType := mType.Out(0)
	errType := mType.Out(1)

	var respVal reflect.Value
	if result != nil {
		respVal = reflect.ValueOf(result)
	} else {
		respVal = reflect.Zero(respType)
	}

	var errVal reflect.Value
	if err != nil {
		errVal = reflect.ValueOf(&err).Elem()
	} else {
		errVal = reflect.Zero(errType)
	}

	return []reflect.Value{respVal, errVal}
}

// packFieldResults packs result + error for function field calls.
func packFieldResults(respType reflect.Type, result any, err error) []reflect.Value {
	errRType := reflect.TypeOf((*error)(nil)).Elem()

	var respVal reflect.Value
	if result != nil {
		respVal = reflect.ValueOf(result)
	} else {
		respVal = reflect.Zero(respType)
	}

	var errVal reflect.Value
	if err != nil {
		errVal = reflect.ValueOf(&err).Elem()
	} else {
		errVal = reflect.Zero(errRType)
	}

	return []reflect.Value{respVal, errVal}
}
