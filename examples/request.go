package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	greptime "github.com/GreptimeTeam/greptimedb-client-go"
)

type monitor struct {
	host   string
	memory float64
	cpu    float64
	ts     time.Time
}

var (
	addr     string = "127.0.0.1:4001"
	table    string = "monitor" // whatever you want
	database string = "public"  // dbname in `GCP`
	username string = ""
	passord  string = ""
)

func main() {
	// Create a new client using an GreptimeDB server base URL and a database name
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := greptime.NewCfg(addr).WithDatabase(database).WithUserName(username).WithPassword(passord).WithDialOptions(options...)

	client, err := greptime.NewClient(cfg)
	if err != nil {
		fmt.Printf("Fail in client initiation, err: %s", err)
	}

	// Create a Series
	series := greptime.Series{}
	series.AddTag("host", "localhost")
	series.SetTimestamp(time.UnixMilli(1660897955002))
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)

	// Create a Metric and add the Series
	metric := greptime.Metric{}
	metric.AddSeries(series)

	// Create an InsertRequest using fluent style
	// If the table does not exist, automatically create one with Insert
	req := greptime.InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithDatabase(database)

	// Do the real Insert and Get the result
	affectedRows, err := client.Insert(context.Background(), req)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
		return
	} else {
		fmt.Printf("affectedRows: %+v\n", affectedRows)
	}

	// Query with metric
	queryReq := greptime.QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)

	resMetric, err := client.Query(context.Background(), queryReq)
	if err != nil {
		fmt.Printf("fail to query, err: %+v\n", err)
		return
	}

	queryMonitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		host, _ := series.Get("host")
		ts, _ := series.GetTimestamp()
		memory, _ := series.Get("memory")
		cpu, _ := series.Get("cpu")
		queryMonitors = append(queryMonitors, monitor{
			host:   host.(string),
			ts:     ts,
			memory: memory.(float64),
			cpu:    cpu.(float64),
		})
	}
	fmt.Printf("Query monitors from db: %+v", queryMonitors)
}
