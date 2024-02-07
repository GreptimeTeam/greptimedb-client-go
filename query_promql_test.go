// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/GreptimeTeam/greptimedb-client-go/prom"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getClient(t *testing.T) *Client {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(host).
		WithPort(grpcPort).
		WithDatabase(database).
		WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, client)
	return client
}

func insert(t *testing.T, client *Client, table string, value float64, secs int64) {
	series := Series{}
	series.AddTag("host", "127.0.0.1")
	series.SetTimestamp(time.Unix(secs, 0))
	series.AddField("val", value)

	metric := Metric{}
	metric.AddSeries(series)

	insert := InsertRequest{}
	insert.WithTable(table).WithMetric(metric)

	inserts := InsertsRequest{}
	inserts.WithDatabase(database).Append(insert)

	resp, err := client.Insert(context.Background(), inserts)
	assert.Nil(t, err)
	assert.True(t, ParseRespHeader(resp).IsSuccess())
	assert.False(t, ParseRespHeader(resp).IsRateLimited())
	assert.Equal(t, uint32(1), resp.GetAffectedRows().GetValue())
}

func TestRangePromql(t *testing.T) {
	table := "test_range_promql"
	var secs int64 = 1677728740
	val := 0.45
	client := getClient(t)
	insert(t, client, table, val, secs)

	rp := NewRangePromql(table).WithStart(time.Unix(secs, 0)).WithEnd(time.Unix(secs, 0)).WithStep(time.Second)
	req := NewQueryRequest().WithRangePromql(rp).WithDatabase(database)
	resp, err := client.PromqlQuery(context.Background(), *req)

	assert.Nil(t, err)
	assert.True(t, ParseRespHeader(resp).IsSuccess())
	assert.False(t, ParseRespHeader(resp).IsRateLimited())

	result, err := prom.UnmarshalApiResponse(resp.GetBody())
	assert.Nil(t, err)
	assert.NotNil(t, result.Val)

	assert.Equal(t, model.ValMatrix, result.Val.Type())
	matrix, ok := result.Val.(model.Matrix)
	assert.True(t, ok)
	assert.Equal(t, 1, matrix.Len())

	sample := matrix[0]
	assert.Equal(t, table, string(sample.Metric["__name__"]))
	assert.Equal(t, 1, len(sample.Values))
	assert.Equal(t, val, float64(sample.Values[0].Value))
}

func TestInstantPromql(t *testing.T) {
	table := "test_instant_promql"
	var secs int64 = 1677728740
	val := 0.45
	client := getClient(t)
	insert(t, client, table, val, secs)

	promql := NewInstantPromql(table).WithTime(time.Unix(secs, 0))
	req := NewQueryRequest().WithInstantPromql(promql)
	resp, err := client.PromqlQuery(context.Background(), *req)

	assert.Nil(t, err)
	assert.True(t, ParseRespHeader(resp).IsSuccess())
	assert.False(t, ParseRespHeader(resp).IsRateLimited())

	result, err := prom.UnmarshalApiResponse(resp.GetBody())
	assert.Nil(t, err)
	assert.NotNil(t, result.Val)

	assert.Equal(t, model.ValVector, result.Val.Type())
	vectors, ok := result.Val.(model.Vector)
	assert.True(t, ok)
	assert.Equal(t, 1, len(vectors))
	vector := vectors[0]

	assert.Equal(t, table, string(vector.Metric["__name__"]))
	assert.Equal(t, val, float64(vector.Value))
}

func TestRangePromqlEmptyStep(t *testing.T) {
	rp := RangePromql{
		Query: "up",
		Start: time.Unix(1677728740, 0),
		End:   time.Unix(1677728740, 0),
	}

	assert.ErrorIs(t, rp.check(), ErrEmptyStep)
}
