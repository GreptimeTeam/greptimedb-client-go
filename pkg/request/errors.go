package request

import "errors"

var (
	EmptyDatabaseError = errors.New("database is required")
	EmptySqlError      = errors.New("sql is required in querying")
)
