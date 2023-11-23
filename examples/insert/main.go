package insert

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
	Host     string // default is 127.0.0.1
	Port     string // default is 4001
	User     string
	Password string
	Database string // default is public

	Client gc.Client
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

	cli, err := gc.NewClient(cfg)
	if err != nil {
		return err
	}

	g.Client = *cli
	return nil
}

func (g *Greptime) Insert() error {
	table := "monitor"
	monitor := Monitor{
		ID:          time.Now().UnixMicro(),
		Host:        "127.0.0.1",
		Ts:          time.Now(),
		Memory:      21,
		Cpu:         0.81,
		Temperature: 21,
	}

	series := gc.Series{}
	series.AddTag("id", monitor.ID)
	series.AddField("host", monitor.Host)
	series.AddField("memory", monitor.Memory)
	series.AddField("cpu", monitor.Cpu)
	series.AddField("temperature", monitor.Temperature)
	series.SetTimestamp(monitor.Ts)

	metric := gc.Metric{}
	metric.SetTimePrecision(time.Microsecond)
	metric.SetTimestampAlias("ts")
	metric.AddSeries(series)

	req := gc.InsertRequest{}
	req.WithTable(table).WithMetric(metric)
	reqs := gc.InsertsRequest{}
	reqs.Append(req)

	resp, err := g.Client.Insert(context.Background(), reqs)
	fmt.Println(resp)
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

	if err := greptimedb.Insert(); err != nil {
		panic(err)
	}

	fmt.Println("insert success via greptimedb-client")
}
