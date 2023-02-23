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
	if err != nil {
		return nil, err
	}

	return &rows{
		reader: reader,
		fields: reader.Schema().Fields(),
	}, nil

}

// driver.StmtExecContext interface
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, errors.New("conn QueryContext args not supported")
	}

	request := req.InsertRequest{
		Header: req.Header{
			Datadase: s.client.Cfg.Database,
		},
	}

	// extract table name from ctx
	table, ok := ctx.Value(req.ContextKeyTable).(string)
	if !ok {
		return nil, fmt.Errorf("fail to convert %+v into a string as table", ctx.Value("table"))
	} else if len(table) == 0 {
		return nil, errors.New("table name is empty")
	}
	request.WithTable(table)

	// extract data from ctx
	data, ok := ctx.Value(req.ContextKeyData).([]any)
	if !ok {
		return nil, fmt.Errorf("fail to convert %+v into []any as data", ctx.Value("data"))
	} else if len(table) == 0 {
		return nil, errors.New("data is empty")
	}
	request.WithData(data)

	// reader, err := s.client.Insert(ctx, req)
	reader, err := s.client.Insert(ctx, request)
	if err != nil {
		return nil, err
	}

	metadata := reader.LatestAppMetadata()
	println("get metadata: %+v from reader", metadata)
	// affectedRows, ok := (metadata).(uint32)
	// if !ok {
	// 	fmt.Printf("fail to convert %+v to uint32 as affectedRows", metadata)
	// 	affectedRows = 0
	// }

	// TODO: reach the result with reader
	// there's not a insertId so far, just fill up with 0
	return &result{
		affectedRows: 0,
		insertId: 0,
	}, nil
}
