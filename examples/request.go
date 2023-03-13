package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
	greptimedb "github.com/GreptimeTeam/greptimedb-client-go/pkg/sql"
)

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
	metric.SetTimePrecision(time.Millisecond)
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

	// Open a GreptimeDB connection with database/sql API.
	// Use `greptimedb` as driverName and a valid DSN to define data source
	db, err := sql.Open("greptimedb", "(127.0.0.1:4001)/public")
	defer db.Close()
	if err != nil {
		fmt.Printf("sql.Open err: %v", err)
		return
	}
	type Monitor struct {
		Host   string
		Cpu    float64
		Memory float64
		Ts     time.Time
	}
	var monitors []Monitor
	greptimedb.Query(db, "SELECT * FROM monitor", &monitors)
	fmt.Printf("%+v\n", monitors)
}
