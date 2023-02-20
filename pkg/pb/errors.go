package pb

import "errors"

var (
	EmptyCatalogError  = errors.New("catalog is required")
	EmptyDatabaseError = errors.New("database is required")
	EmptySqlError      = errors.New("sql is required in querying")
)
