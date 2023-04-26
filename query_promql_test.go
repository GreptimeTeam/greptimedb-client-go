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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestInstantPromqlInPrometheusWay(t *testing.T) {
	// TODO(yuanbohan): waiting for gRPC server
}

func TestRangePromqlInPrometheusWay(t *testing.T) {
	// TODO(yuanbohan): waiting for gRPC server
}

func TestRangePromqlEmptyStep(t *testing.T) {
	rp := RangePromql{
		Query: "up",
		Start: time.Unix(1677728740, 0),
		End:   time.Unix(1677728740, 0),
	}

	assert.ErrorIs(t, rp.check(), ErrEmptyStep)
}

func TestInsertAndQueryWithRangePromQL(t *testing.T) {
	table := "test_insert_and_query_with_range_promql"
	insertMonitors := []monitor{
		{
			host:        "127.0.0.1",
			ts:          time.UnixMilli(1677728740000),
			memory:      22,
			cpu:         0.45,
			temperature: -1,
			isAuthed:    true,
		},
	}

	// init client
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(host).WithPort(port).WithDatabase(database).WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)

	// Insert
	metric := Metric{}
	for _, monitor := range insertMonitors {
		series := Series{}
		series.AddTag("host", monitor.host)
		series.SetTimestamp(monitor.ts)
		series.AddField("memory", monitor.memory)
		series.AddField("cpu", monitor.cpu)
		series.AddField("temperature", monitor.temperature)
		series.AddField("is_authed", monitor.isAuthed)

		metric.AddSeries(series)
	}

	insertReq := InsertRequest{}
	insertReq.WithDatabase(database).WithTable(table).WithMetric(metric)

	n, err := client.Insert(context.Background(), insertReq)
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(insertMonitors)), n)

	// Query with PromQL with metric
	queryReq := &QueryRequest{}
	rp := &RangePromql{Query: table}
	rp.WithStart(time.Unix(1677728740, 0)).WithEnd(time.Unix(1677728740, 0)).WithStep(time.Second * 50)
	queryReq.WithRangePromql(rp).WithDatabase(database)

	resMetric, err := client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resMetric.GetSeries()))

	queryMonitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		host, ok := series.Get("host")
		assert.True(t, ok)
		ts := series.GetTimestamp()

		temperature, ok := series.Get("temperature")
		assert.True(t, ok)
		memory, ok := series.Get("memory")
		assert.True(t, ok)
		cpu, ok := series.Get("cpu")
		assert.True(t, ok)
		isAuthed, ok := series.Get("is_authed")
		assert.True(t, ok)
		queryMonitors = append(queryMonitors, monitor{
			host:        host.(string),
			ts:          ts,
			memory:      memory.(uint64),
			cpu:         cpu.(float64),
			temperature: temperature.(int64),
			isAuthed:    isAuthed.(bool),
		})
	}
	assert.Equal(t, insertMonitors, queryMonitors)
}
