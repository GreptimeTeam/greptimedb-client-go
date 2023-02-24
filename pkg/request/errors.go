package request

import "errors"

var (
	EmptyDatabaseError = errors.New("database is required")
	EmptySqlError      = errors.New("sql is required in querying")
	NilPointerErr      = errors.New("nil")
	TypeNotMatchErr    = errors.New("the dataType should be consistent")
)
