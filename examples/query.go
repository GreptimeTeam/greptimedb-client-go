package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"GreptimeTeam/greptimedb-client-go/pkg/client"
	"GreptimeTeam/greptimedb-client-go/pkg/config"
	"GreptimeTeam/greptimedb-client-go/pkg/pb/query"
)

// TODO(yuanbohan): format the docstring in Go way
// Setup:
//
// 1. docker run -p 4002:4002 greptime/greptimedb standalone start
// 2. mysql -h 127.0.0.1 -P 4002
// 3. create table
// /
// ```mysql
// CREATE TABLE monitor (
//
//	host STRING,
//	ts TIMESTAMP,
//	cpu DOUBLE DEFAULT 0,
//	memory DOUBLE,
//	TIME INDEX (ts),
//	PRIMARY KEY(host)) ENGINE=mito WITH(regions=1);
//
// ```
// 4. go run examples/query.go
func main() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := config.New("127.0.0.1:4001").WithDialOptions(options...)

	client, err := client.New(cfg)
	if err != nil {
		fmt.Printf("Fail in client initiation, err: %s", err)
	}

	req := query.Request{
		Datadase: "public",
		Sql:      "select * from monitor",
	}

	reader, err := client.Query(context.Background(), req)
	defer reader.Release()

	if err != nil {
		fmt.Printf("Fail in Query, err: %s", err)
	}

	for reader.Next() {
		record := reader.Record()
		fmt.Printf("--record--: %+v", record)
	}
}
