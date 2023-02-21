package sql

import (
	"database/sql/driver"

	"github.com/apache/arrow/go/arrow/flight"
)

type rows struct {
	reader *flight.Reader
}

// method of driver.Rows interface
func (r *rows) Columns() []string {
	return nil
}

// method of driver.Rows interface
func (r *rows) Close() error {
	return nil
}

// method of driver.Rows interface
func (r *rows) Next(dest []driver.Value) error {
	return nil
}
