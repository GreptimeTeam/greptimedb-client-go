package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

// should run a standalone greptimedb instance locally
func main() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := request.NewCfg("127.0.0.1:4001", "", "public").WithDialOptions(options...)

	client, err := request.NewClient(cfg)
	if err != nil {
		fmt.Printf("Fail in client initiation, err: %s", err)
	}

	series := request.Series{}
	series.AddTag("host", "localhost")
	series.SetTimeWithKey("ts", time.Now())
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)

	metric := request.Metric{}
	metric.AddSeries(series)

	req := request.InsertRequest{}
	req.WithTable("monitor").WithMetric(metric).WithCatalog("").WithDatabase("public")

	affectedRows, err := client.Insert(context.Background(), req)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
	} else {
		fmt.Printf("affectedRows: %+v\n", affectedRows)
	}
}
