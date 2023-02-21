package sql

import (
	"database/sql/driver"

	"github.com/apache/arrow/go/arrow/flight"
)

type Rows struct {
	reader *flight.Reader
}

// method of driver.Rows interface
func (r *Rows) Columns() []string {
	return nil
}

// method of driver.Rows interface
func (r *Rows) Close() error {
	return nil
}

// method of driver.Rows interface
func (r *Rows) Next(dest []driver.Value) error {
	return nil
}
