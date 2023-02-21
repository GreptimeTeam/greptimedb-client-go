package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connection struct {
	client *req.Client
}

// Prepare is just the interface needed, greptimedb-client-go has no plan for this.
// method of driver.Conn interface
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return nil, driver.ErrSkip

}

// FIXME(yuanbohan): real logic
// method of driver.Conn interface
func (c *connection) Close() error {
	return nil
}

// Begin is just the interface needed, greptimedb-client-go has no plan for this.
// method of driver.Conn interface
func (c *connection) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// TODO(yuanbohan): real logic
func (c *connection) cleanup() {

}

// TODO(yuanbohan): use args
// method of driver.Queryer interface
func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("conn Query args not supported")
	}
	return c.QueryContext(context.Background(), query, nil)
}

// method of driver.QueryerContext interface
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("conn QueryContext args not supported")
	}
	req := req.QueryRequest{
		Header: req.Header{
			Datadase: c.client.Cfg.Database,
		},
		Sql: query,
	}

	reader, err := c.client.Query(ctx, req)
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
