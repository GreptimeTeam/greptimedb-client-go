package sql

import (
	"context"
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

// TODO(yuanbohan): support QueryerContext
// TODO(yuanbohan): use args
// method of driver.Queryer interface
func (c *connection) Query(query string, args []driver.Value) (Rows, error) {
	req := req.QueryRequest{
		Header: req.Header{
			Datadase: "public",
		},
		Sql: "select * from monitor",
	}

	reader, err := c.client.Query(context.Background(), req)
	if reader == nil || err != nil {
		return Rows{}, err
	}
	return Rows{}, nil
}
