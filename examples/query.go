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
	if err != nil {
		fmt.Printf("Fail in Query, err: %s", err)
	}

	for reader.Next() {
		record := reader.Record()
		fmt.Printf("%+v", record)
	}
}
