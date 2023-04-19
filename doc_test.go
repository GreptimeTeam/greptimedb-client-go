package greptime

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Example() {
	// leave `addr`, `database`, `username`, `password` untouched in local machine,
	// but in GreptimeCloud you need to create a service in advance
	addr := "127.0.0.1"
	database := "public"
	username, password := "", "" // authentication of one service

	// replace with your table name
	table := fmt.Sprintf("monitor_%d", time.Now().Unix())

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	// To connect a database that needs authentication, for example, those on Greptime Cloud,
	// `Username` and `Password` are must.
	// To connect a local database without authentication, just leave the two fields empty.
	cfg := NewCfg(addr).
		WithDatabase(database).
		WithAuth(username, password).
		WithDialOptions(options...). // specify your gRPC dail options
		WithCallOptions()            // specify your gRPC call options

	client, err := NewClient(cfg)
	if err != nil {
		panic("failed to init client")
	}

	// inserting
	series := Series{}                       // Create one row of data
	series.AddStringTag("host", "localhost") // add index column, for query efficiency
	series.AddFloatField("cpu", 0.90)        // add value column
	series.AddIntField("memory", 1024)       // add value column
	series.SetTimestamp(time.Now())          // requird

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
	fmt.Printf("AffectedRows: %d\n", n)

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

	type Monitor struct {
		host   string
		cpu    float64
		memory int64
		ts     time.Time
	}

	monitors := []Monitor{}
	for _, series := range resMetric.GetSeries() {
		one := &Monitor{}
		host, exist := series.Get("host") // you can directly call Get and do the type assertion
		if exist {
			one.host = host.(string)
		}
		one.cpu, _ = series.GetFloat("cpu")     // also, you can directly GetFloat
		one.memory, _ = series.GetInt("memory") // also, you can directly GetInt
		one.ts = series.GetTimestamp()          // GetTimestamp
		monitors = append(monitors, *one)
	}
	fmt.Println(monitors)
}
