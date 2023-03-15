package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

type monitor struct {
	host   string
	memory float64
	cpu    float64
	ts     time.Time
}

func main() {
	// Create a new client using an GreptimeDB server base URL and a database name
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := request.NewCfg("127.0.0.1:4001", "", "public").WithDialOptions(options...)

	client, err := request.NewClient(cfg)
	if err != nil {
		fmt.Printf("Fail in client initiation, err: %s", err)
	}

	// Create a Series
	series := request.Series{}
	series.AddTag("host", "localhost")
	series.SetTimeWithKey("ts", time.UnixMilli(1660897955000))
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)

	// Create a Metric and add the Series
	metric := request.Metric{}
	metric.AddSeries(series)

	// Create an InsertRequest using fluent style
	// If the table does not exist, automatically create one with Insert
	req := request.InsertRequest{}
	req.WithTable("monitor").WithMetric(metric).WithCatalog("").WithDatabase("public")

	// Do the real Insert and Get the result
	affectedRows, err := client.Insert(context.Background(), req)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
		return
	} else {
		fmt.Printf("affectedRows: %+v\n", affectedRows)
	}

	// Query with metric
	queryReq := request.QueryRequest{}
	queryReq.WithSql("SELECT * FROM monitor").WithCatalog("").WithDatabase("public")

	resMetric, err := client.QueryMetric(context.Background(), queryReq)
	if err != nil {
		fmt.Printf("fail to query, err: %+v\n", err)
		return
	}

	queryMonitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		host, _ := series.Get("host")
		ts, _ := series.Get("ts")
		memory, _ := series.Get("memory")
		cpu, _ := series.Get("cpu")
		queryMonitors = append(queryMonitors, monitor{
			host:   host.(string),
			ts:     time.UnixMilli(ts.(int64)),
			memory: memory.(float64),
			cpu:    cpu.(float64),
		})
	}
	fmt.Printf("Query monitors from db: %+v", queryMonitors)
}
