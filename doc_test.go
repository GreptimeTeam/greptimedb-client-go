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

package greptime

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Example() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	// To connect a database that needs authentication, for example, those on Greptime Cloud,
	// `Username` and `Password` are needed when connecting to a database that requires authentication.
	// Leave the two fields empty if connecting a database without authentication.
	cfg := NewCfg("127.0.0.1").
		WithPort(4001).              // default is 4001.
		WithDatabase("public").      // specify your database
		WithAuth("", "").            // specify Username,Password for authentication enabled GreptimeDB
		WithDialOptions(options...). // specify your gRPC dail options
		WithCallOptions()            // specify your gRPC call options
	client, err := NewClient(cfg)
	if err != nil {
		panic("failed to init client")
	}

	table := "monitor"

	// inserting
	series := Series{}
	// Tag is index column, for query efficiency
	series.AddTag("region", "az")            // type is checked automatically
	series.AddStringTag("host", "localhost") // type is specified by user
	// Field is value column
	series.AddFloatField("cpu", 0.90) // type is specified by user
	series.AddField("memory", 1024)   // type is checked automatically
	// Timestamp is required
	series.SetTimestamp(time.Now())

	metric := Metric{}
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

	// querying
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
		region string
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
		one.region, _ = series.GetString("region")
		one.cpu, _ = series.GetFloat("cpu")     // also, you can directly GetFloat
		one.memory, _ = series.GetInt("memory") // also, you can directly GetInt
		one.ts = series.GetTimestamp()          // GetTimestamp
		monitors = append(monitors, *one)
	}
	fmt.Println(monitors)
}
