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
	series := Series{}                 // Create one row of data
	series.AddTag("host", "localhost") // index, for query efficiency
	series.AddField("cpu", 0.90)       // value
	series.AddField("memory", 1024.0)  // value
	series.SetTimestamp(time.Now())    // requird

	metric := Metric{} // Create a Metric and add the Series
	metric.AddSeries(series)

	// Create an InsertRequest using fluent style
	// the specified table will be created automatically if it's not exist
	insertRequest := InsertRequest{}
	// if you want to specify another database, you can specify it via: `WithDatabase(database)`
	insertRequest.WithTable(table).WithMetric(metric) // .WithDatabase(database)

	// Fire the real Insert request and Get the affected number of rows
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
	queryRequest.WithSql("SELECT * FROM " + table) // .WithDatabase(database)

	resMetric, err := client.Query(context.Background(), queryRequest)
	if err != nil {
		fmt.Printf("fail to query, err: %+v\n", err)
		return
	}

	monitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		one := &monitor{}
		host, exist := series.Get("host") // you can directly call Get and do the type assertion
		if exist {
			one.host = host.(string)
		}
		one.memory, _ = series.GetFloat("memory") // you can directly GetFloat
		one.cpu, _ = series.GetFloat("cpu")       // you can directly GetFloat
		one.ts = series.GetTimestamp()            // GetTimestamp
		monitors = append(monitors, *one)
	}
	fmt.Printf("Query monitors from db: %+v\n", monitors)
}
