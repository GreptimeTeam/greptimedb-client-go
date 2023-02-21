package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connection struct {
	cfg    *req.Config
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
func (c *connection) Query(sql string, args []driver.Value) (*Rows, error) {
	req := req.QueryRequest{
		Header: req.Header{
			Datadase: c.cfg.Database,
		},
		Sql: sql,
	}

	fmt.Printf("----connection----: %#v", c)

	reader, err := c.client.Query(context.Background(), req)
	defer reader.Release()
	if err != nil {
		return nil, err
	}

	for reader.Next() {
		record := reader.Record()
		fmt.Printf("--record--: %+v", record)
	}

	return &Rows{reader}, nil
}
