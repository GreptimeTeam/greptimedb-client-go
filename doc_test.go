// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package greptime_test

import (
	"context"
	"fmt"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-client-go"
	"github.com/GreptimeTeam/greptimedb-client-go/prom"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	client *greptime.Client

	monitorTable string = "monitor"
)

func init() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := greptime.NewCfg("127.0.0.1").
		WithPort(4001).
		WithDatabase("public").
		WithAuth("", "").
		WithDialOptions(options...).
		WithCallOptions()
	c, err := greptime.NewClient(cfg)
	if err != nil {
		panic("failed to init client")
	}
	client = c
}

func constructInsertRequest(table string) greptime.InsertRequest {
	series := greptime.Series{}
	series.AddTag("region", "az")
	series.AddStringTag("host", "localhost")
	series.AddFloatField("cpu", 0.90)
	series.AddField("memory", 1024)
	series.SetTimestamp(time.Now())

	metric := greptime.Metric{}
	metric.AddSeries(series)

	insertRequest := greptime.InsertRequest{}
	insertRequest.WithTable(table).WithMetric(metric)

	return insertRequest
}

func ExampleInsert() {
	insertsRequest := greptime.InsertsRequest{}
	insertsRequest.
		Append(constructInsertRequest(monitorTable)).
		Append(constructInsertRequest("temperatures"))

	resp, err := client.Insert(context.Background(), insertsRequest)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
		return
	}
	fmt.Printf("AffectedRows: %d\n", resp.GetAffectedRows().GetValue())
}

func ExampleQueryViaSql() {
	type Monitor struct {
		region string
		host   string
		cpu    float64
		memory int64
		ts     time.Time
	}

	req := greptime.QueryRequest{}
	req.WithSql("SELECT * FROM " + monitorTable)

	resMetric, err := client.Query(context.Background(), req)
	if err != nil {
		fmt.Printf("fail to query, err: %+v\n", err)
		return
	}

	monitors := []Monitor{}
	for _, series := range resMetric.GetSeries() {
		monitor := &Monitor{}
		host, exist := series.Get("host")
		if exist {
			monitor.host = host.(string)
		}
		monitor.region, _ = series.GetString("region")
		monitor.cpu, _ = series.GetFloat("cpu")
		monitor.memory, _ = series.GetInt("memory")
		monitor.ts, _ = series.GetTimestamp("ts")
		monitors = append(monitors, *monitor)
	}
	fmt.Println(monitors)
}

func ExampleQueryViaInstantPromql() {
	promql := greptime.NewInstantPromql(monitorTable)
	req := greptime.QueryRequest{}
	req.WithInstantPromql(promql)
	resp, err := client.PromqlQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("failed to do instant promql query: %+v\n", err)
		return
	}

	result, err := prom.UnmarshalApiResponse(resp.GetBody())
	if err != nil {
		fmt.Printf("failed to unmarshal instant promql, body: %s, err: %+v", string(resp.GetBody()), err)
		return
	}
	fmt.Printf("%s:\n%+v\n", result.Type, result.Val)

}

func ExamplQueryViaRangePromql() {
	end := time.Now()
	start := end.Add(time.Duration(-15) * time.Second)
	promql := greptime.NewRangePromql(monitorTable).WithStart(start).WithEnd(end).WithStep(time.Second)
	req := greptime.QueryRequest{}
	req.WithRangePromql(promql)
	resp, err := client.PromqlQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("failed to do range promql query: %+v\n", err)
		return
	}

	result, err := prom.UnmarshalApiResponse(resp.GetBody())
	if err != nil {
		fmt.Printf("failed to unmarshal instant promql, body: %s, err: %+v", string(resp.GetBody()), err)
		return
	}
	fmt.Printf("%s:\n%+v\n", result.Type, result.Val)
}
