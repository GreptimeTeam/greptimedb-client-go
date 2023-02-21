package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

// TODO(yuanbohan): format the docstring in Go way
// Setup:
//
// 1. docker run -p 4001:4001 -p 4002:4002 greptime/greptimedb standalone start --mysql-addr=0.0.0.0:4002 --rpc-addr=0.0.0.0:4001
// 2. mysql -h 127.0.0.1 -P 4002
// 3. create table
// /
// ```mysql
// CREATE TABLE monitor (

//	host STRING,
//	ts TIMESTAMP,
//	cpu DOUBLE DEFAULT 0,
//	memory DOUBLE,
//	TIME INDEX (ts),
//	PRIMARY KEY(host)) ENGINE=mito WITH(regions=1);
//
// 4. insert data
// INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host1', 66.6, 1024, 1660897955000);
// INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host2', 77.7, 2048, 1660897956000);
// INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host3', 88.8, 4096, 1660897957000);
// ```
// 5. go run examples/query.go
func main() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := req.NewCfg("127.0.0.1:4001").WithDialOptions(options...)

	client, err := req.NewClient(cfg)
	if err != nil {
		fmt.Printf("Fail in client initiation, err: %s", err)
	}

	req := req.QueryRequest{
		Header: req.Header{
			Datadase: "public",
		},
		Sql: "select * from monitor",
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
