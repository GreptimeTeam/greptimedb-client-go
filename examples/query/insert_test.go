package main

import (
	req "GreptimeTeam/greptimedb-client-go/pkg/request"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	db, err := sql.Open("greptimedb", "(127.0.0.1:4001)/public")

	if err != nil {
		fmt.Printf("sql.Open err: %v", err)
	}

	monitors := []*Monitor{
		{"host1", time.Unix(1660897955, 0), 66.6, 1024},
		{"host2", time.Unix(1660897966, 0), 77.7, 2048},
		{"host3", time.Unix(1660897977, 0), 88.8, 3072},
	}

	// TODO(vinland-avalon): wrap this "processing with context" into a function
	ctx := context.WithValue(context.Background(), req.ContextKeyTable, "monitor")
	ctx = context.WithValue(ctx, req.ContextKeyData, monitors)

	res, err := db.ExecContext(ctx, "")

	if err != nil {
		fmt.Printf("db.Query err: %v", err)
	}

	println("insert result: %+v", res)
}
