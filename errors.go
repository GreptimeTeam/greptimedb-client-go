package greptime

import (
	"errors"
)

var (
	ErrEmptyDatabase        = errors.New("name of database should not be empty")
	ErrEmptyTable           = errors.New("name of table should not be be empty")
	ErrEmptyTimestamp       = errors.New("timestamp should not be empty")
	ErrEmptyKey             = errors.New("key should not be empty")
	ErrEmptyQuery           = errors.New("sql or promql is required in querying")
	ErrEmptyStep            = errors.New("step is required in promql")
	ErrInvalidTimePrecision = errors.New("precision of timestamp is not valid")
	ErrNoSeriesInMetric     = errors.New("empty series in Metric")
)
