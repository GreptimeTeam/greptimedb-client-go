package greptime

// import (
//	"context"
//	"fmt"
//	"testing"
//	"time"

//	"github.com/stretchr/testify/assert"
//	"google.golang.org/grpc"
//	"google.golang.org/grpc/credentials/insecure"
// )

// func TestStreamInsert(t *testing.T) {
//	table := "test_stream_insert"
//	genBatchInsertionData := func(size int) []monitor {
//		monitors := make([]monitor, 0, 10)
//		for i := 0; i < size; i++ {
//			one := monitor{
//				host:        "127.0.0.1",
//				ts:          time.Now(),
//				memory:      22,
//				cpu:         0.45,
//				temperature: -1,
//				isAuthed:    true,
//			}

//			monitors = append(monitors, one)
//			time.Sleep(time.Millisecond)
//		}
//		return monitors
//	}

//	// Insert
//	options := []grpc.DialOption{
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//	}
//	cfg := NewCfg(grpcAddr).WithDatabase(database).WithDialOptions(options...)
//	streamClient, err := NewStreamClient(cfg)
//	assert.Nil(t, err)

//	size := 200
//	insertMonitors := genBatchInsertionData(size)
//	for _, monitor := range insertMonitors {
//		metric := Metric{}

//		series := Series{}
//		series.AddTag("host", monitor.host)
//		series.SetTimestamp(monitor.ts)
//		series.AddField("memory", monitor.memory)
//		series.AddField("cpu", monitor.cpu)
//		series.AddField("temperature", monitor.temperature)
//		series.AddField("is_authed", monitor.isAuthed)
//		metric.AddSeries(series)

//		req := InsertRequest{}
//		req.WithTable(table).WithMetric(metric)
//		err = streamClient.Send(context.Background(), req)
//		assert.Nil(t, err)
//	}

//	affectedRows, err := streamClient.CloseAndRecv(context.Background())
//	assert.Nil(t, err)
//	assert.Equal(t, size, affectedRows.Value)

//	// Query with metric
//	queryReq := QueryRequest{}
//	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table)).WithDatabase(database)

//	client, err := NewClient(cfg)
//	assert.Nil(t, err)
//	resMetric, err := client.Query(context.Background(), queryReq)
//	assert.Nil(t, err)
//	assert.Equal(t, 2, len(resMetric.GetSeries()))

//	queryMonitors := []monitor{}
//	for _, series := range resMetric.GetSeries() {
//		host, ok := series.Get("host")
//		assert.True(t, ok)
//		ts, ok := series.GetTimestamp()
//		assert.True(t, ok)
//		temperature, ok := series.Get("temperature")
//		assert.True(t, ok)
//		memory, ok := series.Get("memory")
//		assert.True(t, ok)
//		cpu, ok := series.Get("cpu")
//		assert.True(t, ok)
//		isAuthed, ok := series.Get("is_authed")
//		assert.True(t, ok)
//		queryMonitors = append(queryMonitors, monitor{
//			host:        host.(string),
//			ts:          ts,
//			memory:      memory.(uint64),
//			cpu:         cpu.(float64),
//			temperature: temperature.(int64),
//			isAuthed:    isAuthed.(bool),
//		})
//	}
//	assert.Equal(t, insertMonitors, queryMonitors)
// }
