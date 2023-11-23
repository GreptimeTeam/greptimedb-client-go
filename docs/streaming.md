Streaming Inserting
==

```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	gc "github.com/GreptimeTeam/greptimedb-client-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Monitor struct {
	ID          int64
	Host        string
	Memory      uint64
	Cpu         float64
	Temperature int64
	Ts          time.Time
}

type Greptime struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string

	StreamClient gc.StreamClient
}

func mockData(size int) []Monitor {
	monitors := make([]Monitor, 0, size)
	for i := 0; i < size; i++ {
		monitor := Monitor{
			ID:          time.Now().UnixMicro(),
			Host:        "127.0.0.1",
			Ts:          time.Now(),
			Memory:      21,
			Cpu:         0.81,
			Temperature: 21,
		}

		monitors = append(monitors, monitor)
		time.Sleep(time.Millisecond)
	}
	return monitors
}

func (g *Greptime) Setup() error {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	cfg := gc.NewCfg(g.Host).
		WithDatabase(g.Database).
		WithAuth(g.User, g.Password).
		WithDialOptions(options...)

	if len(g.Port) > 0 {
		port, err := strconv.Atoi(g.Port)
		if err != nil {
			return err
		}
		cfg.WithPort(port)
	}

	cli, err := gc.NewStreamClient(cfg)
	if err != nil {
		return err
	}

	g.StreamClient = *cli
	return nil
}

func (g *Greptime) StreamInsert() error {
	table := "monitor"

	size := 20
	insertMonitors := mockData(size)
	metric := gc.Metric{}
	for i, monitor := range insertMonitors {
		series := gc.Series{}
		series.AddTag("id", monitor.ID)
		series.AddField("host", monitor.Host)
		series.AddField("memory", monitor.Memory)
		series.AddField("cpu", monitor.Cpu)
		series.AddField("temperature", monitor.Temperature)
		series.SetTimestamp(monitor.Ts)
		metric.AddSeries(series)

		if len(metric.GetSeries()) > 0 {
			if (i+1)%10 == 0 || i >= size-1 {
				req := gc.InsertRequest{}
				req.WithTable(table).WithMetric(metric)

				reqs := gc.InsertsRequest{}
				reqs.Append(req)

				fmt.Printf("ready to send %d records\n", len(metric.GetSeries()))
				if err := g.StreamClient.Send(context.Background(), reqs); err != nil {
					fmt.Println(err)
				}
				metric = gc.Metric{}
			}
		}
	}
	_, err := g.StreamClient.CloseAndRecv(context.Background())
	return err
}

func main() {
	greptimedb := &Greptime{
		Host:     "127.0.0.1",
		Port:     "4001",
		User:     "",
		Password: "",
		Database: "public",
	}
	if err := greptimedb.Setup(); err != nil {
		panic(err)
	}

	greptimedb.StreamInsert()
	fmt.Println("stream insert success via greptimedb-client")
}

```