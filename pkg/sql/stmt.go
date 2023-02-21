package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type stmt struct {
	client *req.Client
	query  string
}

func (s *stmt) Close() error {
	return driver.ErrSkip
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, errors.New("stmt.Exec args not supported")
	}
	return s.ExecContext(context.Background(), nil)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("stmt.Query args not supported")
	}
	return s.QueryContext(context.Background(), nil)
}

// driver.StmtQueryContext interface
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	if len(args) > 0 {
		return nil, errors.New("conn QueryContext args not supported")
	}
	req := req.QueryRequest{
		Header: req.Header{
			Datadase: s.client.Cfg.Database,
		},
		Sql: s.query,
	}

	reader, err := s.client.Query(ctx, req)
	defer reader.Release()
	if err != nil {
		return nil, err
	}

	for reader.Next() {
		record := reader.Record()
		fmt.Printf("--record--: %+v", record)
	}

	return &rows{reader}, nil

}

// driver.StmtExecContext interface
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("stmt.ExecContext not implemented!")
}
