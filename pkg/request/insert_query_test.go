package request

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	database string = "public"
	table    string = "monitor"
)

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

type monitor struct {
	host        string
	memory      uint64
	cpu         float64
	temperature int64
	ts          time.Time
	isAuthed    bool
}

func TestBasicWorkFlow(t *testing.T) {
	grpcAddr := DockerTestInit(DefaultDockerTestConfig())

	insertMonitors := []monitor{
		{
			host:        "127.0.0.1",
			ts:          time.UnixMilli(1677728740000),
			memory:      22,
			cpu:         0.45,
			temperature: -1,
			isAuthed:    true,
		},
		{
			host:        "127.0.0.2",
			ts:          time.UnixMilli(1677728740012),
			memory:      28,
			cpu:         0.80,
			temperature: 22,
			isAuthed:    true,
		},
	}

	// Insert
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := NewCfg(grpcAddr, "", database).WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)

	metric := Metric{}
	metric.SetTimePrecision(time.Microsecond)
	for _, monitor := range insertMonitors {
		series := Series{}
		series.AddTag("host", monitor.host)
		series.SetTimeWithKey("ts", monitor.ts)
		series.AddField("memory", monitor.memory)
		series.AddField("cpu", monitor.cpu)
		series.AddField("temperature", monitor.temperature)
		series.AddField("is_authed", monitor.isAuthed)
		metric.AddSeries(series)
	}

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithCatalog("").WithDatabase(database)

	affectedRows, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(insertMonitors)), affectedRows.Value)

	// Query with metric
	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithCatalog("").WithDatabase(database)

	resMetric, err := client.QueryMetric(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(resMetric.GetSeries()))

	queryMonitors := []monitor{}
	for _, series := range resMetric.GetSeries() {
		host, ok := series.Get("host")
		assert.True(t, ok)
		ts, ok := series.Get("ts")
		assert.True(t, ok)
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
			ts:          time.UnixMicro(ts.(int64)),
			memory:      memory.(uint64),
			cpu:         cpu.(float64),
			temperature: temperature.(int64),
			isAuthed:    isAuthed.(bool),
		})
	}
	assert.Equal(t, insertMonitors, queryMonitors)
}

func TestDataTypes(t *testing.T) {
	grpcAddr := DockerTestInit(DefaultDockerTestConfig())

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
	cfg := NewCfg(grpcAddr, "", database).WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)

	metric := Metric{}

	series := Series{}
	series.AddTag("int64_v", data.int64V)
	series.AddTag("int32_v", data.int32V)
	series.AddTag("int16_v", data.int16V)
	series.AddTag("int8_v", data.int8V)
	series.AddTag("int_v", data.intV)
	series.AddTag("uint64_v", data.uint64V)
	series.AddField("uint32_v", data.uint32V)
	series.AddField("uint16_v", data.uint16V)
	series.AddField("uint8_v", data.uint8V)
	series.AddField("uint_v", data.uintV)
	series.AddField("float64_v", data.float64V)
	series.AddField("float32_v", data.float32V)
	series.AddField("string_v", data.stringV)
	series.AddField("byte_v", data.byteV)
	series.AddField("bool_v", data.boolV)
	series.SetTimeWithKey("time_v", data.timeV)
	metric.AddSeries(series)

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithCatalog("").WithDatabase(database)

	affectedRows, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), affectedRows.Value)

	// Query with metric
	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithCatalog("").WithDatabase(database)

	resMetric, err := client.QueryMetric(context.Background(), queryReq)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resMetric.GetSeries()))

	series = resMetric.GetSeries()[0]
	int64V, ok := series.Get("int64_v")
	assert.True(t, ok)
	int32V, ok := series.Get("int32_v")
	assert.True(t, ok)
	int16V, ok := series.Get("int16_v")
	assert.True(t, ok)
	int8V, ok := series.Get("int8_v")
	assert.True(t, ok)
	intV, ok := series.Get("int_v")
	assert.True(t, ok)
	uint64V, ok := series.Get("uint64_v")
	assert.True(t, ok)
	uint32V, ok := series.Get("uint32_v")
	assert.True(t, ok)
	uint16V, ok := series.Get("uint16_v")
	assert.True(t, ok)
	uint8V, ok := series.Get("uint8_v")
	assert.True(t, ok)
	uintV, ok := series.Get("uint_v")
	assert.True(t, ok)
	float64V, ok := series.Get("float64_v")
	assert.True(t, ok)
	float32V, ok := series.Get("float32_v")
	assert.True(t, ok)
	stringV, ok := series.Get("string_v")
	assert.True(t, ok)
	byteV, ok := series.Get("byte_v")
	assert.True(t, ok)
	boolV, ok := series.Get("bool_v")
	assert.True(t, ok)
	timeV, ok := series.Get("time_v")
	assert.True(t, ok)

	querydata := datatype{
		int64V:   int64V.(int64),
		int32V:   int32V.(int32),
		int16V:   int16(int16V.(int32)),
		int8V:    int8(int8V.(int32)),
		intV:     int(intV.(int64)),
		uint64V:  uint64V.(uint64),
		uint32V:  uint32V.(uint32),
		uint16V:  uint16(uint16V.(uint32)),
		uint8V:   uint8(uint8V.(uint32)),
		uintV:    uint(uintV.(uint64)),
		float64V: float64V.(float64),
		float32V: float32(float32V.(float64)),
		stringV:  stringV.(string),
		byteV:    []byte(byteV.(string)),
		boolV:    boolV.(bool),
		timeV:    time.UnixMilli(timeV.(int64)),
	}
	assert.Equal(t, data, querydata)
}
