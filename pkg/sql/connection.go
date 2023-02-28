package sql

import (
	"context"
	"database/sql/driver"

	req "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connection struct {
	client *req.Client
}

// Prepare is just the interface needed, greptimedb-client-go has no plan for this.
// method of driver.Conn interface
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// TODO(yuanbohan): real logic
// method of driver.Conn interface
func (c *connection) Close() error {
	return nil
}

// Begin is just the interface needed, greptimedb-client-go has no plan for this.
// method of driver.Conn interface
func (c *connection) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// driver.ConnPrepareContext interface
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// TODO(yuanbohan): use the ctx parameter
	return &stmt{
		client: c.client,
		query:  query,
	}, nil
}
