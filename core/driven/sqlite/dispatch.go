package sqlite

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dispatch routes the request to the appropriate GORM operation based on Op.
func dispatch(tx *gorm.DB, req any, reqElemType, respType reflect.Type, op Op) (any, error) {
	switch op {
	case OpCreate:
		return dispatchCreate(tx, req, reqElemType, respType)
	case OpFind:
		return dispatchFind(tx, req, reqElemType, respType)
	case OpList:
		return dispatchList(tx, req, reqElemType, respType)
	case OpUpdate:
		return dispatchUpdate(tx, req, reqElemType, respType)
	case OpDelete:
		return dispatchDelete(tx, req, reqElemType, respType)
	case OpCount:
		return dispatchCount(tx, req, reqElemType, respType)
	case OpExec:
		return dispatchExec(tx, req, reqElemType, respType)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", op)
	}
}

// dispatchCreate handles INSERT operations.
// The Req struct should contain the data to insert, the Result is the inserted record.
func dispatchCreate(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	q := buildQuery(tx, req)

	// If request has a Model() method, use its return value
	model := extractModel(req)
	if model != nil {
		result := q.Create(model)
		if result.Error != nil {
			return nil, result.Error
		}
		return model, nil
	}

	// Otherwise use the request itself as the model to create
	result := q.Create(req)
	if result.Error != nil {
		return nil, result.Error
	}
	return req, nil
}

// dispatchFind handles SELECT single-record operations.
func dispatchFind(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	q := buildQuery(tx, req)

	// Create instance of response type
	resp := newInstance(respType)

	// Apply where conditions from request tags
	q = applyConditions(q, req)
	q = applyPreloads(q, req)

	result := q.First(resp)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, result.Error
	}

	return resp, nil
}

// dispatchList handles SELECT multiple-record operations.
func dispatchList(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	q := buildQuery(tx, req)

	// Apply where conditions and pagination
	q = applyConditions(q, req)
	q = applyPagination(q, req)
	q = applyOrdering(q, req)
	q = applyPreloads(q, req)

	// Create a slice of the response's element type
	resp := newSliceInstance(respType)

	result := q.Find(resp)
	if result.Error != nil {
		return nil, result.Error
	}

	return resp, nil
}

// dispatchUpdate handles UPDATE operations.
func dispatchUpdate(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	q := buildQuery(tx, req)

	// Apply where conditions
	q = applyConditions(q, req)

	// Get the update values from the request
	updates := extractUpdates(req)
	if updates == nil {
		// Use the request itself as the update source
		model := extractModel(req)
		if model != nil {
			result := q.Updates(model)
			if result.Error != nil {
				return nil, result.Error
			}
			return model, nil
		}
		result := q.Updates(req)
		if result.Error != nil {
			return nil, result.Error
		}
		return req, nil
	}

	result := q.Updates(updates)
	if result.Error != nil {
		return nil, result.Error
	}

	// Return the count of affected rows in a wrapper
	count := result.RowsAffected
	resp := newInstance(respType)
	setRowsAffected(resp, count)
	return resp, nil
}

// dispatchDelete handles DELETE operations.
func dispatchDelete(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	q := buildQuery(tx, req)

	// Apply where conditions
	q = applyConditions(q, req)

	// Need a model for Delete()
	model := extractModel(req)
	if model == nil {
		model = newInstance(respType)
	}

	result := q.Delete(model)
	if result.Error != nil {
		return nil, result.Error
	}

	resp := newInstance(respType)
	setRowsAffected(resp, result.RowsAffected)
	return resp, nil
}

// dispatchCount handles COUNT operations.
func dispatchCount(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	q := buildQuery(tx, req)
	q = applyConditions(q, req)

	var count int64
	result := q.Count(&count)
	if result.Error != nil {
		return nil, result.Error
	}

	resp := newInstance(respType)
	setCountField(resp, count)
	return resp, nil
}

// dispatchExec handles raw SQL execution.
func dispatchExec(tx *gorm.DB, req any, reqType, respType reflect.Type) (any, error) {
	sql, args := extractRawSQL(req)
	if sql == "" {
		return nil, fmt.Errorf("exec operation requires SQL field in request")
	}

	resp := newInstance(respType)
	result := tx.Raw(sql, args...).Scan(resp)
	if result.Error != nil {
		return nil, result.Error
	}

	return resp, nil
}

// --- Query Building Helpers ---

// buildQuery sets up the base gorm.DB query with table name and model.
func buildQuery(tx *gorm.DB, req any) *gorm.DB {
	// Check if request defines a table name via the Table() method
	if t, ok := req.(interface{ Table() string }); ok {
		return tx.Table(t.Table())
	}

	// Check if request defines a Model() method
	if m, ok := req.(interface{ Model() any }); ok {
		return tx.Model(m.Model())
	}

	// Check for `db:"table:xxx"` tag on a TableName field
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() == reflect.Struct {
		reqType := reqVal.Type()
		for i := 0; i < reqType.NumField(); i++ {
			field := reqType.Field(i)
			tag := field.Tag.Get("db")
			if strings.HasPrefix(tag, "table:") {
				tableName := strings.TrimPrefix(tag, "table:")
				return tx.Table(tableName)
			}
		}
	}

	return tx
}

// applyConditions reads `where` tagged fields from the request and adds WHERE clauses.
func applyConditions(q *gorm.DB, req any) *gorm.DB {
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() != reflect.Struct {
		return q
	}

	reqType := reqVal.Type()
	for i := 0; i < reqType.NumField(); i++ {
		field := reqType.Field(i)
		fieldVal := reqVal.Field(i)

		tag := field.Tag.Get("where")
		if tag == "" {
			continue
		}

		// Skip zero-value fields unless tagged with `required`
		if isZero(fieldVal) {
			continue
		}

		// Parse tag: "column_name" or "column_name op" where op can be =, !=, >, <, >=, <=, like, in
		parts := strings.SplitN(tag, " ", 2)
		column := parts[0]
		operator := "="
		if len(parts) > 1 {
			operator = parts[1]
		}

		value := fieldVal.Interface()

		switch strings.ToLower(operator) {
		case "=", "eq":
			q = q.Where(fmt.Sprintf("%s = ?", column), value)
		case "!=", "ne", "neq":
			q = q.Where(fmt.Sprintf("%s != ?", column), value)
		case ">", "gt":
			q = q.Where(fmt.Sprintf("%s > ?", column), value)
		case "<", "lt":
			q = q.Where(fmt.Sprintf("%s < ?", column), value)
		case ">=", "gte":
			q = q.Where(fmt.Sprintf("%s >= ?", column), value)
		case "<=", "lte":
			q = q.Where(fmt.Sprintf("%s <= ?", column), value)
		case "like":
			q = q.Where(fmt.Sprintf("%s LIKE ?", column), value)
		case "in":
			q = q.Where(fmt.Sprintf("%s IN ?", column), value)
		case "is null", "null":
			q = q.Where(fmt.Sprintf("%s IS NULL", column))
		case "is not null", "not null":
			q = q.Where(fmt.Sprintf("%s IS NOT NULL", column))
		default:
			q = q.Where(fmt.Sprintf("%s %s ?", column, operator), value)
		}
	}

	return q
}

// applyPagination reads `page` and `limit` tagged fields.
func applyPagination(q *gorm.DB, req any) *gorm.DB {
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() != reflect.Struct {
		return q
	}

	reqType := reqVal.Type()
	var limit, offset int

	for i := 0; i < reqType.NumField(); i++ {
		field := reqType.Field(i)
		fieldVal := reqVal.Field(i)

		switch field.Tag.Get("page") {
		case "limit":
			if v, ok := toInt(fieldVal); ok && v > 0 {
				limit = v
			}
		case "offset":
			if v, ok := toInt(fieldVal); ok && v >= 0 {
				offset = v
			}
		case "page":
			if v, ok := toInt(fieldVal); ok && v > 0 {
				// page is 1-based, convert to offset
				if limit > 0 {
					offset = (v - 1) * limit
				}
			}
		}
	}

	if limit > 0 {
		q = q.Limit(limit)
	}
	if offset > 0 {
		q = q.Offset(offset)
	}

	return q
}

// applyOrdering reads `order` tagged fields.
func applyOrdering(q *gorm.DB, req any) *gorm.DB {
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() != reflect.Struct {
		return q
	}

	reqType := reqVal.Type()
	for i := 0; i < reqType.NumField(); i++ {
		field := reqType.Field(i)
		fieldVal := reqVal.Field(i)

		tag := field.Tag.Get("order")
		if tag == "" {
			continue
		}

		// If the field is a string and has a value, use it as the order clause
		if fieldVal.Kind() == reflect.String && fieldVal.String() != "" {
			q = q.Order(fieldVal.String())
		} else if tag != "" {
			// Use the tag value as a static order clause
			q = q.Order(tag)
		}
	}

	return q
}

// applyPreloads reads `preload` tagged fields.
func applyPreloads(q *gorm.DB, req any) *gorm.DB {
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() != reflect.Struct {
		return q
	}

	reqType := reqVal.Type()
	for i := 0; i < reqType.NumField(); i++ {
		field := reqType.Field(i)

		tag := field.Tag.Get("preload")
		if tag == "" {
			continue
		}

		if tag == "*" {
			q = q.Preload(clause.Associations)
		} else {
			for _, assoc := range strings.Split(tag, ",") {
				q = q.Preload(strings.TrimSpace(assoc))
			}
		}
	}

	return q
}

// --- Value Extraction Helpers ---

// extractModel checks if request implements Model() any interface.
func extractModel(req any) any {
	if m, ok := req.(interface{ Model() any }); ok {
		return m.Model()
	}

	// Look for an embedded "Data" field
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() == reflect.Struct {
		dataField := reqVal.FieldByName("Data")
		if dataField.IsValid() && !isZero(dataField) {
			return dataField.Interface()
		}
	}
	return nil
}

// extractUpdates checks if request implements Updates() map[string]any interface.
func extractUpdates(req any) map[string]any {
	if u, ok := req.(interface{ Updates() map[string]any }); ok {
		return u.Updates()
	}

	// Look for `update` tagged fields
	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() != reflect.Struct {
		return nil
	}

	updates := make(map[string]any)
	reqType := reqVal.Type()
	for i := 0; i < reqType.NumField(); i++ {
		field := reqType.Field(i)
		fieldVal := reqVal.Field(i)

		tag := field.Tag.Get("update")
		if tag == "" {
			continue
		}
		if !isZero(fieldVal) {
			updates[tag] = fieldVal.Interface()
		}
	}

	if len(updates) == 0 {
		return nil
	}
	return updates
}

// extractRawSQL checks if request has SQL and Args fields.
func extractRawSQL(req any) (string, []any) {
	if s, ok := req.(interface{ SQL() (string, []any) }); ok {
		return s.SQL()
	}

	reqVal := reflect.ValueOf(req)
	if reqVal.Kind() == reflect.Ptr {
		reqVal = reqVal.Elem()
	}
	if reqVal.Kind() != reflect.Struct {
		return "", nil
	}

	sqlField := reqVal.FieldByName("SQL")
	if !sqlField.IsValid() || sqlField.Kind() != reflect.String {
		return "", nil
	}

	var args []any
	argsField := reqVal.FieldByName("Args")
	if argsField.IsValid() && argsField.Kind() == reflect.Slice {
		for i := 0; i < argsField.Len(); i++ {
			args = append(args, argsField.Index(i).Interface())
		}
	}

	return sqlField.String(), args
}

// --- Instance Creation Helpers ---

// newInstance creates a new pointer instance of the given type.
// If respType is *T, it creates a new T and returns *T.
func newInstance(t reflect.Type) any {
	if t.Kind() == reflect.Ptr {
		return reflect.New(t.Elem()).Interface()
	}
	return reflect.New(t).Interface()
}

// newSliceInstance creates a pointer to a new slice for the response type.
// If respType is *[]T, creates &[]T{}
func newSliceInstance(t reflect.Type) any {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		sliceVal := reflect.MakeSlice(t, 0, 0)
		ptr := reflect.New(t)
		ptr.Elem().Set(sliceVal)
		return ptr.Interface()
	}
	// If not a slice, create a slice of the type
	sliceType := reflect.SliceOf(t)
	sliceVal := reflect.MakeSlice(sliceType, 0, 0)
	ptr := reflect.New(sliceType)
	ptr.Elem().Set(sliceVal)
	return ptr.Interface()
}

// setRowsAffected tries to set a RowsAffected field on the response.
func setRowsAffected(resp any, count int64) {
	val := reflect.ValueOf(resp)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return
	}
	field := val.FieldByName("RowsAffected")
	if field.IsValid() && field.CanSet() && field.Kind() == reflect.Int64 {
		field.SetInt(count)
	}
}

// setCountField tries to set a Count field on the response.
func setCountField(resp any, count int64) {
	val := reflect.ValueOf(resp)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return
	}

	// Try "Count" field first, then "Total"
	for _, name := range []string{"Count", "Total"} {
		field := val.FieldByName(name)
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.Int64 {
			field.SetInt(count)
			return
		}
	}
}

// --- Utility Helpers ---

func isZero(v reflect.Value) bool {
	return v.IsZero()
}

func toInt(v reflect.Value) (int, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(v.Uint()), true
	}
	return 0, false
}
