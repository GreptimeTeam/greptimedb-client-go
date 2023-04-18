package main

import (
	"context"
	"fmt"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-client-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	client *greptime.Client
)

func init() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	// To connect a database that needs authentication, for example, those on Greptime Cloud,
	// `Username` and `Password` are must.
	// To connect a local database without authentication, just leave the two fields empty.
	cfg := greptime.NewCfg(addr).
		WithDatabase(database).
		WithAuth(username, password).
		WithDialOptions(options...)

	c, err := greptime.NewClient(cfg)
	if err != nil {
		panic("failed to init client")
	}
	client = c
}

func insert() {
	// Create a Series
	series := greptime.Series{}
	series.AddTag("host", "localhost")
	series.SetTimestamp(time.Now()) // requird
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)

	// Create a Metric and add the Series
	metric := greptime.Metric{}
	metric.AddSeries(series)

	// Create an InsertRequest using fluent style
	// If the table does not exist, automatically create one with Insert
	req := greptime.InsertRequest{}
	// if you want to specify another database, you can specify it via: `req.WithDatabase(database)`
	req.WithTable(table).WithMetric(metric)

	// Do the real Insert and Get the result
	n, err := client.Insert(context.Background(), req)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
		return
	}
	fmt.Printf("Success! AffectedRows: %d\n", n)
}

func query() {
	// Query with metric via Sql, you can do it via PromQL
	req := greptime.QueryRequest{}
	// if you want to specify another database, you can specify it via: `req.WithDatabase(database)`
	req.WithSql(fmt.Sprintf("SELECT * FROM %s", table))

	resMetric, err := client.Query(context.Background(), req)
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

func main() {
	// insert()
	query()
}
