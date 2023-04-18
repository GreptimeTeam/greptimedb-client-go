package greptime

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Example() {
	type monitor struct {
		host   string
		memory float64
		cpu    float64
		ts     time.Time
	}

	var (
		addr     string = "127.0.0.1"
		table    string = "monitor" // whatever you want
		database string = "public"  // dbname in `GCP`
		username string = ""
		password string = ""
	)

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	// To connect a database that needs authentication, for example, those on Greptime Cloud,
	// `Username` and `Password` are must.
	// To connect a local database without authentication, just leave the two fields empty.
	cfg := NewCfg(addr).
		WithDatabase(database).
		WithAuth(username, password).
		WithDialOptions(options...)

	client, err := NewClient(cfg)
	if err != nil {
		panic("failed to init client")
	}

	// inserting
	// Create a Series
	series := Series{}
	series.AddTag("host", "localhost")
	series.SetTimestamp(time.Now()) // requird
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)

	// Create a Metric and add the Series
	metric := Metric{}
	metric.AddSeries(series)

	// Create an InsertRequest using fluent style
	// If the table does not exist, automatically create one with Insert
	insertRequest := InsertRequest{}
	// if you want to specify another database, you can specify it via: `WithDatabase(database)`
	insertRequest.WithTable(table).WithMetric(metric)

	// Do the real Insert and Get the result
	n, err := client.Insert(context.Background(), insertRequest)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
		return
	}
	fmt.Printf("Success! AffectedRows: %d\n", n)

	// quering
	// Query with metric via Sql, you can do it via PromQL
	queryRequest := QueryRequest{}
	// if you want to specify another database, you can specify it via: `WithDatabase(database)`
	queryRequest.WithSql("SELECT * FROM " + table)

	resMetric, err := client.Query(context.Background(), queryRequest)
	if err != nil {
		fmt.Printf("fail to query, err: %+v\n", err)
		return
	}

	monitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		host, _ := series.Get("host")
		ts := series.GetTimestamp()
		memory, _ := series.Get("memory")
		cpu, _ := series.Get("cpu")
		monitors = append(monitors, monitor{
			host:   host.(string),
			ts:     ts,
			memory: memory.(float64),
			cpu:    cpu.(float64),
		})
	}
	fmt.Printf("Query monitors from db: %+v\n", monitors)

}
