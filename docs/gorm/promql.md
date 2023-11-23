Retrieving via PromQL
==

```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	gc "github.com/GreptimeTeam/greptimedb-client-go"
	"github.com/GreptimeTeam/greptimedb-client-go/prom"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var monitorTable = "monitor"

type Greptime struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string

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

// the response format is in []byte, and is absolutely the same as Prometheus
func (g *Greptime) queryViaInstantPromql() {
	promql := gc.NewInstantPromql(monitorTable)
	req := gc.QueryRequest{}
	req.WithInstantPromql(promql)
	resp, err := g.Client.PromqlQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("failed to do instant promql query: %+v\n", err)
		return
	}

	// you can use prom package to unmarshal the response as you want
	result, err := prom.UnmarshalApiResponse(resp.GetBody())
	if err != nil {
		fmt.Printf("failed to unmarshal instant promql, body: %s, err: %+v", string(resp.GetBody()), err)
		return
	}
	fmt.Printf("%s:\n%+v\n", result.Type, result.Val)

}

// the response format is in []byte, and is absolutely the same as Prometheus
func (g *Greptime) queryViaRangePromql() {
	end := time.Now()
	start := end.Add(time.Duration(-15) * time.Second) // 15 seconds before
	promql := gc.NewRangePromql(monitorTable).WithStart(start).WithEnd(end).WithStep(time.Second)
	req := gc.QueryRequest{}
	req.WithRangePromql(promql)
	resp, err := g.Client.PromqlQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("failed to do range promql query: %+v\n", err)
		return
	}

	// you can use prom package to unmarshal the response as you want
	result, err := prom.UnmarshalApiResponse(resp.GetBody())
	if err != nil {
		fmt.Printf("failed to unmarshal instant promql, body: %s, err: %+v", string(resp.GetBody()), err)
		return
	}
	fmt.Printf("%s:\n%+v\n", result.Type, result.Val)
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

	greptimedb.queryViaInstantPromql()
	greptimedb.queryViaRangePromql()
}

```