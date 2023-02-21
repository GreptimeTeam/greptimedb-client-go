package sql

import (
	"database/sql/driver"
	"errors"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connection struct {
	client *req.Client
}

// Prepare is just the interface needed, greptimedb-client-go has no plan for this.
// method of driver.Conn interface
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Prepare(string) not implemented!")

}

// FIXME(yuanbohan): real logic
// method of driver.Conn interface
func (c *connection) Close() error {
	return nil
}

// Begin is just the interface needed, greptimedb-client-go has no plan for this.
// method of driver.Conn interface
func (c *connection) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin() not implemented!")
}

// TODO(yuanbohan): real logic
func (c *connection) cleanup() {

}

func (c *connection) Query(query string, args []driver.Value) (Rows, error) {
	// TODO(yuanbohan): use args
	return Rows{}, nil
}
