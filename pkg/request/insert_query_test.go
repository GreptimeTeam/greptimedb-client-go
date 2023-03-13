package request

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	pool     *dockertest.Pool
	resource *dockertest.Resource

	grpcAddr string
	database string = "public"
	repo     string = "greptime/greptimedb"
	tag      string = "0.1.0"
	// table    string = "monitor"
	table2 string = "datatype"
)

type datatype struct {
	int64V   int64
	int32V   int32
	int16V   int16
	int8V    int8
	uint64V  uint64
	uint32V  uint32
	uint16V  uint16
	uint8V   uint8
	float64V float64
	float32V float32
	stringV  string
	byteV    []byte
	boolV    bool
	timeV    time.Time
}

func init() {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.WithFields(log.Fields{
		"repository": repo,
		"tag":        tag,
	}).Infof("Preparing container %s:%s !!!!", repo, tag)

	// pulls an image, creates a container based on it and runs it
	resource, err = pool.RunWithOptions(&dockertest.RunOptions{
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
		grpcAddr = resource.GetHostPort("4001/tcp")
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
}

func TestBasicWorkFlow(t *testing.T) {
	data := datatype{
		int64V:   64,
		int32V:   32,
		int16V:   16,
		int8V:    8,
		uint64V:  64,
		uint32V:  32,
		uint16V:  16,
		uint8V:   8,
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
	// cfg := NewCfg("localstringV:4001", "", database).WithDialOptions(options...)

	client, err := NewClient(cfg)
	assert.Nil(t, err)

	metric := Metric{}

	series := Series{}
	series.AddTag("int64_v", data.int64V)
	series.AddTag("int32_v", data.int32V)
	series.AddTag("int16_v", data.int16V)
	series.AddTag("int8_v", data.int8V)
	series.AddTag("uint64_v", data.uint64V)
	series.AddField("uint32_v", data.uint32V)
	series.AddField("uint16_v", data.uint16V)
	series.AddField("uint8_v", data.uint8V)
	series.AddField("float64_v", data.float64V)
	series.AddField("float32_v", data.float32V)
	series.AddField("string_v", data.stringV)
	series.AddField("byte_v", data.byteV)
	series.AddField("bool_v", data.boolV)
	series.SetTimeWithKey("time_v", data.timeV)
	metric.AddSeries(series)

	req := InsertRequest{}
	req.WithTable(table2).WithMetric(metric).WithCatalog("").WithDatabase(database)

	affectedRows, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), affectedRows.Value)

	// Query with metric
	queryReq := QueryRequest{}
	queryReq.WithSql(fmt.Sprintf("SELECT * FROM %s", table2)).WithCatalog("").WithDatabase(database)

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
	uint64V, ok := series.Get("uint64_v")
	assert.True(t, ok)
	uint32V, ok := series.Get("uint32_v")
	assert.True(t, ok)
	uint16V, ok := series.Get("uint16_v")
	assert.True(t, ok)
	uint8V, ok := series.Get("uint8_v")
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
		uint64V:  uint64V.(uint64),
		uint32V:  uint32V.(uint32),
		uint16V:  uint16(uint16V.(uint32)),
		uint8V:   uint8(uint8V.(uint32)),
		float64V: float64V.(float64),
		float32V: float32(float32V.(float64)),
		stringV:  stringV.(string),
		byteV:    []byte(byteV.(string)),
		boolV:    boolV.(bool),
		timeV:    time.UnixMilli(timeV.(int64)),
	}

	assert.Equal(t, data, querydata)
}
