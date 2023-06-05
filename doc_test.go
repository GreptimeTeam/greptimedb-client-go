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

	// monitorTable used in this Example, you don't have to create it in advance,
	// if the monitorTable not exist, it will be created automatically.
	monitorTable string = "monitor"
)

// initClient creates a client with config.
//
// `Username` and `Password` are needed when connecting to a database that requires authentication.
// Leave the two fields empty if connecting a database without authentication.
func initClient() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := greptime.NewCfg("127.0.0.1").
		WithPort(4001).         // default is 4001.
		WithDatabase("public"). // specify your database
		WithAuth("", "").
		WithDialOptions(options...). // specify your gRPC dail options
		WithCallOptions()            // specify your gRPC call options
	c, err := greptime.NewClient(cfg)
	if err != nil {
		panic("failed to init client")
	}
	client = c
}

// insert one Series
//   - Tag is index column, for query efficiency
//   - Field is value column
//   - Timestamp is required
//
// you can specify the column type, or let the type checking to be done by sdk
//
//   - series.AddTag
//   - series.AddField
//   - series.AddXxxTag
//   - series.AddXxxField
func constructInsertRequest(table string) greptime.InsertRequest {
	series := greptime.Series{}
	series.AddTag("region", "az")            // type is checked automatically
	series.AddStringTag("host", "localhost") // type is specified by user
	series.AddFloatField("cpu", 0.90)        // type is specified by user
	series.AddField("memory", 1024)          // type is checked automatically
	series.SetTimestamp(time.Now())

	metric := greptime.Metric{}
	metric.AddSeries(series)

	// Create an InsertRequest using fluent style
	// the specified table will be created automatically if it's not exist
	insertRequest := greptime.InsertRequest{}
	insertRequest.WithTable(table).WithMetric(metric)

	return insertRequest
}

func insert() {
	insertsRequest := greptime.InsertsRequest{}
	// You can insert data of different tables into greptimedb in one InsertsRequest.
	// This insertsRequest includes two InsertRequest of two different tables
	insertsRequest.
		Insert(constructInsertRequest(monitorTable)).
		Insert(constructInsertRequest("temperatures"))

	// if you want to insert into different table in one request, you can construct
	// another InsertRequest, and include it via: insertsRequest.Insert(insertRequest)

	// if you want to specify another database, you can specify it via: `WithDatabase(database)`
	// insertsRequest.WithDatabase("your database")

	// Fire the real Inserts request and Get the affected number of rows
	n, err := client.Insert(context.Background(), insertsRequest)
	if err != nil {
		fmt.Printf("fail to insert, err: %+v\n", err)
		return
	}
	fmt.Printf("AffectedRows: %d\n", n)
}

// queryViaSql via Sql
//
// you can get the column by type, or do the type conversion by yourself
//
//   - series.Get     // you have to do the conversion explitely
//   - series.GetXxx  // GetFloat, GetInt, GetUint, GetString, GetBool, GetBytes
func queryViaSql() {
	// Monitor is the metrics used in this Example
	type Monitor struct {
		region string
		host   string
		cpu    float64
		memory int64
		ts     time.Time
	}

	req := greptime.QueryRequest{}
	// if you want to specify another database, you can specify it via: `WithDatabase(database)`
	req.WithSql("SELECT * FROM " + monitorTable) // .WithDatabase(database)

	resMetric, err := client.Query(context.Background(), req)
	if err != nil {
		fmt.Printf("fail to query, err: %+v\n", err)
		return
	}

	monitors := []Monitor{}
	for _, series := range resMetric.GetSeries() {
		monitor := &Monitor{}
		host, exist := series.Get("host") // you can directly call Get and do the type assertion
		if exist {
			monitor.host = host.(string)
		}
		monitor.region, _ = series.GetString("region")
		monitor.cpu, _ = series.GetFloat("cpu")     // also, you can directly GetFloat
		monitor.memory, _ = series.GetInt("memory") // also, you can directly GetInt
		monitor.ts = series.GetTimestamp()          // GetTimestamp
		monitors = append(monitors, *monitor)
	}
	fmt.Println(monitors)
}

// the response format is in []byte, and is absolutely the same as Prometheus
func queryViaInstantPromql() {
	promql := greptime.NewInstantPromql(monitorTable)
	req := greptime.QueryRequest{}
	req.WithInstantPromql(promql)
	resp, err := client.PromqlQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("failed to do instant promql query: %+v\n", err)
		return
	}

	// you can use prom package to unmarshal the response as you want
	result, err := prom.UnmarshalApiResponse(resp)
	if err != nil {
		fmt.Printf("failed to unmarshal instant promql, body: %s, err: %+v", string(resp), err)
		return
	}
	fmt.Printf("%s:\n%+v\n", result.Type, result.Val)

}

// the response format is in []byte, and is absolutely the same as Prometheus
func queryViaRangePromql() {
	end := time.Now()
	start := end.Add(time.Duration(-15) * time.Second) // 15 seconds before
	promql := greptime.NewRangePromql(monitorTable).WithStart(start).WithEnd(end).WithStep(time.Second)
	req := greptime.QueryRequest{}
	req.WithRangePromql(promql)
	resp, err := client.PromqlQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("failed to do range promql query: %+v\n", err)
		return
	}

	// you can use prom package to unmarshal the response as you want
	result, err := prom.UnmarshalApiResponse(resp)
	if err != nil {
		fmt.Printf("failed to unmarshal instant promql, body: %s, err: %+v", string(resp), err)
		return
	}
	fmt.Printf("%s:\n%+v\n", result.Type, result.Val)
}

func Example() {
	initClient()
	insert()

	queryViaSql()
	queryViaInstantPromql()
	queryViaRangePromql()
}
