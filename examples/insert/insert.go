package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"GreptimeTeam/greptimedb-client-go/pkg/request"
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
	series.AddTag("host", "localhost10")
	series.SetTimeWithKey("ts", time.Now())
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)
	// series.SetTime(time.Now())

	metric := request.Metric{}
	metric.AddSeries(series)

	req := request.InsertRequest{}
	req.WithTable("monitor3").WithMetric(metric).WithCatalog("").WithDatabase("public")

	client.Insert(context.Background(), req)
}
