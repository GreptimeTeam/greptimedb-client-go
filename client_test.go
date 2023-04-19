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
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type monitor struct {
	host        string
	memory      uint64
	cpu         float64
	temperature int64
	ts          time.Time
	isAuthed    bool
}

var (
	database = "public"
	addr     = "127.0.0.1"
	port     = 0
)

func init() {
	repo := "greptime/greptimedb"
	tag := "0.2.0-nightly-20230328"

	var err error
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.WithFields(log.Fields{
		"repository": repo,
		"tag":        tag,
	}).Infof("Preparing container %s:%s", repo, tag)

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   repo,
		Tag:          tag,
		ExposedPorts: []string{"4001", "4002"},
		Entrypoint:   []string{"greptime", "standalone", "start", "--rpc-addr=0.0.0.0:4001", "--mysql-addr=0.0.0.0:4002"},
	}, func(config *dc.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = dc.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	var expire uint = 30
	log.WithFields(log.Fields{
		"repository": repo,
		"tag":        tag,
		"expire":     expire,
	}).Infof("Container starting...")

	err = resource.Expire(expire) // Tell docker to hard kill the container
	if err != nil {
		log.WithError(nil).Warn("Expire container failed")
	}

	pool.MaxWait = 60 * time.Second

	if err := pool.Retry(func() error {
		// TODO(vinland-avalon): some functions, like ping() to check if container is ready
		time.Sleep(time.Second)
		port, err = strconv.Atoi(resource.GetPort(("4001/tcp")))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
}

func TestInsertAndQueryWithSql(t *testing.T) {
	table := "test_insert_and_query_with_sql"
	insertMonitors := []monitor{
		{
			host:        "127.0.0.1",
			ts:          time.UnixMicro(1677728740000001),
			memory:      22,
			cpu:         0.45,
			temperature: -1,
			isAuthed:    true,
		},
		{
			host:        "127.0.0.2",
			ts:          time.UnixMicro(1677728740012002),
			memory:      28,
			cpu:         0.80,
			temperature: 22,
			isAuthed:    true,
		},
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(addr).WithPort(port).WithDatabase(database).WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)

	metric := Metric{}
	metric.SetTimePrecision(time.Microsecond)
	metric.SetTimestampAlias("ts")

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

	req := InsertRequest{}
	req.WithDatabase(database).WithTable(table).WithMetric(metric)

	n, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(insertMonitors)), n)

	// Query with metric
	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)

	resMetric, err := client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(resMetric.GetSeries()))

	queryMonitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		host, ok := series.GetString("host")
		assert.True(t, ok)
		temperature, ok := series.GetInt("temperature")
		assert.True(t, ok)
		memory, ok := series.GetUint("memory")
		assert.True(t, ok)
		cpu, ok := series.GetFloat("cpu")
		assert.True(t, ok)
		isAuthed, ok := series.GetBool("is_authed")
		assert.True(t, ok)

		ts := series.GetTimestamp()
		queryMonitors = append(queryMonitors, monitor{
			host:        host,
			ts:          ts,
			memory:      memory,
			cpu:         cpu,
			temperature: temperature,
			isAuthed:    isAuthed,
		})
	}
	assert.Equal(t, insertMonitors, queryMonitors)

	// query but no data
	queryReq = QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s WHERE host = 'not_exist'", table)).WithDatabase(database)

	resMetric, err = client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(resMetric.GetSeries()))
}

func TestPrecisionSecond(t *testing.T) {
	table := "test_precision_second"
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(addr).WithPort(port).WithDatabase(database).WithDialOptions(options...)

	client, err := NewClient(cfg)
	assert.Nil(t, err)

	nano := time.Unix(1677728740, 123456789)
	micro := time.UnixMicro(nano.UnixMicro())
	milli := time.UnixMilli(nano.UnixMilli())
	sec := time.Unix(nano.Unix(), 0)

	series := Series{}
	series.SetTimestamp(nano)
	metric := Metric{}
	metric.AddSeries(series)
	// We set the precision as second
	metric.SetTimePrecision(time.Second)
	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithDatabase(database)
	n, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), n)

	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)
	resMetric, err := client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resMetric.GetSeries()))

	resTime := resMetric.GetSeries()[0].GetTimestamp()
	// since the precision is second, others should not equal
	assert.Equal(t, sec, resTime)
	assert.NotEqual(t, milli, resTime)
	assert.NotEqual(t, nano, resTime)
	assert.NotEqual(t, micro, resTime)
}

func TestNilInColumn(t *testing.T) {
	table := "test_nil_in_column"

	insertMonitors := []monitor{
		{
			ts:  time.UnixMicro(1677728740000001),
			cpu: 0.45,
		},
		{
			ts:     time.UnixMicro(1677728740012002),
			memory: 28,
		},
	}

	// Insert
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(addr).WithPort(port).WithDatabase(database).WithDialOptions(options...)

	client, err := NewClient(cfg)
	assert.Nil(t, err)

	metric := Metric{}
	metric.SetTimePrecision(time.Microsecond)

	series1 := Series{}
	series1.SetTimestamp(insertMonitors[0].ts)
	series1.AddField("cpu", insertMonitors[0].cpu)
	metric.AddSeries(series1)

	series2 := Series{}
	series2.SetTimestamp(insertMonitors[1].ts)
	series2.AddField("memory", insertMonitors[1].memory)
	metric.AddSeries(series2)

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithDatabase(database)

	n, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(insertMonitors)), n)

	// Query with metric
	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)

	resMetric, err := client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(resMetric.GetSeries()))

	resSeries0 := resMetric.GetSeries()[0]
	ts := resSeries0.GetTimestamp()

	assert.Equal(t, insertMonitors[0].ts, ts)
	_, ok := resSeries0.Get("memory")
	assert.False(t, ok)
	cpu, ok := resSeries0.Get("cpu")
	assert.True(t, ok)
	assert.Equal(t, insertMonitors[0].cpu, cpu.(float64))

	resSeries1 := resMetric.GetSeries()[1]
	ts = resSeries1.GetTimestamp()

	assert.Equal(t, insertMonitors[1].ts, ts)
	memory, ok := resSeries1.Get("memory")
	assert.True(t, ok)
	assert.Equal(t, insertMonitors[1].memory, memory.(uint64))
	_, ok = resSeries1.Get("cpu")
	assert.False(t, ok)
}

func TestNoNeedAuth(t *testing.T) {
	table := "test_no_need_auth"
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	// Client can always connect to a no-auth database, even the usernames and passwords are wrong
	cfg := NewCfg(addr).WithPort(port).WithDatabase(database).WithAuth("user", "pwd").WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)

	nano := time.Unix(1677728740, 123456789)

	series := Series{}
	series.SetTimestamp(nano)
	metric := Metric{}
	metric.AddSeries(series)

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithDatabase(database)
	n, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), n)

	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)
	resMetric, err := client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resMetric.GetSeries()))

	resTime := resMetric.GetSeries()[0].GetTimestamp()
	// since the precision is second, others should not equal
	assert.NotEqual(t, nano, resTime)
}

func TestDataTypes(t *testing.T) {
	table := "test_data_types"
	type datatype struct {
		int64V   int64
		int32V   int32
		int16V   int16
		int8V    int8
		intV     int
		uint64V  uint64
		uint32V  uint32
		uint16V  uint16
		uint8V   uint8
		uintV    uint
		float64V float64
		float32V float32
		stringV  string
		byteV    []byte
		boolV    bool
		timeV    time.Time
	}

	data := datatype{
		int64V:   64,
		int32V:   32,
		int16V:   16,
		int8V:    8,
		intV:     64,
		uint64V:  64,
		uint32V:  32,
		uint16V:  16,
		uint8V:   8,
		uintV:    64,
		float64V: 64.0,
		float32V: 32.0,
		stringV:  "string",
		byteV:    []byte("byte"),
		boolV:    true,
		timeV:    time.UnixMilli(1677728740012),
	}

	// Insert
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(addr).WithPort(port).WithDatabase(database).WithDialOptions(options...)

	client, err := NewClient(cfg)
	assert.Nil(t, err)

	metric := Metric{}
	metric.SetTimestampAlias("time_v")

	series := Series{}
	// int
	assert.Nil(t, series.AddIntTag("int64_v_tag", data.int64V))
	assert.Nil(t, series.AddTag("int32_v_tag", data.int32V))
	assert.Nil(t, series.AddTag("int16_v_tag", data.int16V))
	assert.Nil(t, series.AddTag("int8_v_tag", data.int8V))
	assert.Nil(t, series.AddTag("int_v_tag", data.intV))
	assert.Nil(t, series.AddIntField("int64_v_field", data.int64V))
	assert.Nil(t, series.AddField("int32_v_field", data.int32V))
	assert.Nil(t, series.AddField("int16_v_field", data.int16V))
	assert.Nil(t, series.AddField("int8_v_field", data.int8V))
	assert.Nil(t, series.AddField("int_v_field", data.intV))

	// uint
	assert.Nil(t, series.AddUintTag("uint64_v_tag", data.uint64V))
	assert.Nil(t, series.AddTag("uint32_v_tag", data.uint32V))
	assert.Nil(t, series.AddTag("uint16_v_tag", data.uint16V))
	assert.Nil(t, series.AddTag("uint8_v_tag", data.uint8V))
	assert.Nil(t, series.AddTag("uint_v_tag", data.uintV))
	assert.Nil(t, series.AddUintField("uint64_v_field", data.uint64V))
	assert.Nil(t, series.AddField("uint32_v_field", data.uint32V))
	assert.Nil(t, series.AddField("uint16_v_field", data.uint16V))
	assert.Nil(t, series.AddField("uint8_v_field", data.uint8V))
	assert.Nil(t, series.AddField("uint_v_field", data.uintV))

	// float
	assert.Nil(t, series.AddFloatTag("float64_v_tag", data.float64V))
	assert.Nil(t, series.AddTag("float32_v_tag", data.float32V))
	assert.Nil(t, series.AddFloatField("float64_v_field", data.float64V))
	assert.Nil(t, series.AddField("float32_v_field", data.float32V))

	// string
	assert.Nil(t, series.AddStringTag("string_v_tag", data.stringV))
	assert.Nil(t, series.AddStringField("string_v_field", data.stringV))

	// bytes
	assert.Nil(t, series.AddBytesTag("byte_v_tag", data.byteV))
	assert.Nil(t, series.AddBytesField("byte_v_field", data.byteV))

	// bool
	assert.Nil(t, series.AddBoolTag("bool_v_tag", data.boolV))
	assert.Nil(t, series.AddBoolField("bool_v_field", data.boolV))

	assert.Nil(t, series.SetTimestamp(data.timeV))
	metric.AddSeries(series)

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithDatabase(database)

	n, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), n)

	// Query with metric
	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)

	resMetric, err := client.Query(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resMetric.GetSeries()))

	series = resMetric.GetSeries()[0]
	// int
	int64V, ok := series.GetInt("int64_v_tag")
	assert.True(t, ok)
	int32V, ok := series.GetInt("int32_v_tag")
	assert.True(t, ok)
	int16V, ok := series.GetInt("int16_v_tag")
	assert.True(t, ok)
	int8V, ok := series.GetInt("int8_v_tag")
	assert.True(t, ok)
	intV, ok := series.GetInt("int_v_tag")
	assert.True(t, ok)

	_, ok = series.GetInt("int64_v_field")
	assert.True(t, ok)
	_, ok = series.GetInt("int32_v_field")
	assert.True(t, ok)
	_, ok = series.GetInt("int16_v_field")
	assert.True(t, ok)
	_, ok = series.GetInt("int8_v_field")
	assert.True(t, ok)
	_, ok = series.GetInt("int_v_field")
	assert.True(t, ok)

	// uint
	uint64V, ok := series.GetUint("uint64_v_tag")
	assert.True(t, ok)
	uint32V, ok := series.GetUint("uint32_v_tag")
	assert.True(t, ok)
	uint16V, ok := series.GetUint("uint16_v_tag")
	assert.True(t, ok)
	uint8V, ok := series.GetUint("uint8_v_tag")
	assert.True(t, ok)
	uintV, ok := series.GetUint("uint_v_tag")
	assert.True(t, ok)

	_, ok = series.GetUint("uint64_v_field")
	assert.True(t, ok)
	_, ok = series.GetUint("uint32_v_field")
	assert.True(t, ok)
	_, ok = series.GetUint("uint16_v_field")
	assert.True(t, ok)
	_, ok = series.GetUint("uint8_v_field")
	assert.True(t, ok)
	_, ok = series.GetUint("uint_v_field")
	assert.True(t, ok)

	// float
	float64V, ok := series.GetFloat("float64_v_tag")
	assert.True(t, ok)
	float32V, ok := series.GetFloat("float32_v_tag")
	assert.True(t, ok)

	_, ok = series.GetFloat("float64_v_field")
	assert.True(t, ok)
	_, ok = series.GetFloat("float32_v_field")
	assert.True(t, ok)

	// string
	stringV, ok := series.GetString("string_v_tag")
	assert.True(t, ok)

	_, ok = series.GetString("string_v_field")
	assert.True(t, ok)

	// bytes
	byteV, ok := series.GetBytes("byte_v_tag")
	assert.True(t, ok)

	_, ok = series.GetBytes("byte_v_field")
	assert.True(t, ok)

	// bool
	boolV, ok := series.GetBool("bool_v_tag")
	assert.True(t, ok)

	_, ok = series.GetBool("bool_v_field")
	assert.True(t, ok)

	timeV := series.GetTimestamp()

	querydata := datatype{
		int64V:   int64V,
		int32V:   int32(int32V),
		int16V:   int16(int16V),
		int8V:    int8(int8V),
		intV:     int(intV),
		uint64V:  uint64V,
		uint32V:  uint32(uint32V),
		uint16V:  uint16(uint16V),
		uint8V:   uint8(uint8V),
		uintV:    uint(uintV),
		float64V: float64V,
		float32V: float32(float32V),
		stringV:  stringV,
		byteV:    byteV,
		boolV:    boolV,
		timeV:    timeV,
	}
	assert.Equal(t, data, querydata)
}
