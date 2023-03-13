package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

var (
	pool     *dockertest.Pool
	resource *dockertest.Resource

	grpcAddr   string
	database   string = "public"
	driverName string = "greptimedb"
	repo       string = "greptime/greptimedb"
	tag        string = "0.1.0"
	table      string = "monitor"
)

type monitor struct {
	Host        string
	Memory      uint64
	Cpu         float64
	Temperature int64
	Ts          time.Time
	IsAuthed    bool
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
	insertMonitor := []monitor{
		{
			Host:        "Beijing",
			Ts:          time.UnixMilli(1677728740000),
			Temperature: 22,
			Memory:      1024,
			IsAuthed:    true,
			Cpu:         0.9,
		},
		{
			Host:        "Shanghai",
			Ts:          time.UnixMilli(1677728740012),
			Temperature: -1,
			Memory:      2048,
			IsAuthed:    false,
			Cpu:         0.5,
		},
	}
	// Insert
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := request.NewCfg(grpcAddr, "", database).WithDialOptions(options...)

	client, err := request.NewClient(cfg)
	assert.Nil(t, err)

	metric := request.Metric{}
	for _, monitor := range insertMonitor {
		series := request.Series{}
		series.AddTag("host", monitor.Host)
		series.SetTimeWithKey("ts", monitor.Ts)
		series.AddField("temperature", monitor.Temperature)
		series.AddField("memory", monitor.Memory)
		series.AddField("cpu", monitor.Cpu)
		series.AddField("is_authed", monitor.IsAuthed)
		metric.AddSeries(series)
	}

	req := request.InsertRequest{}
	req.WithTable(table).WithMetric(metric).WithCatalog("").WithDatabase(database)

	affectedRows, err := client.Insert(context.Background(), req)
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(insertMonitor)), affectedRows.Value)

	// Query with database/sql
	db, err := sql.Open(driverName, fmt.Sprintf("(%s)/%s", grpcAddr, database))
	assert.Nil(t, err)

	res, err := db.Query(fmt.Sprintf("SELECT * FROM %s", table))
	assert.Nil(t, err)

	var queryMonitors []monitor
	for res.Next() {
		var queryMonitor monitor
		err = res.Scan(&queryMonitor.Host, &queryMonitor.Temperature,
			&queryMonitor.Memory, &queryMonitor.Cpu, &queryMonitor.IsAuthed, &queryMonitor.Ts)
		assert.Nil(t, err)
		queryMonitors = append(queryMonitors, queryMonitor)
	}

	assert.Nil(t, err)
	assert.Equal(t, insertMonitor, queryMonitors)

	// Query with slice
	queryMonitors2 := []monitor{}
	err = Query(db, fmt.Sprintf("SELECT * FROM %s", table), &queryMonitors2)
	assert.Nil(t, err)
	assert.Equal(t, insertMonitor, queryMonitors2)

	// Query with slice -- random order of returned data
	type monitorRandomOrder struct {
		// move Memory above Host
		Memory      uint64
		Host        string
		Cpu         float64
		Temperature int64
		Ts          time.Time
		IsAuthed    bool
	}
	expectedMonitorsRandomOrder := []monitorRandomOrder{
		{
			Host:        "Beijing",
			Ts:          time.UnixMilli(1677728740000),
			Temperature: 22,
			Memory:      1024,
			IsAuthed:    true,
			Cpu:         0.9,
		},
		{
			Host:        "Shanghai",
			Ts:          time.UnixMilli(1677728740012),
			Temperature: -1,
			Memory:      2048,
			IsAuthed:    false,
			Cpu:         0.5,
		},
	}
	queryMonitorsRandomOrder := []monitorRandomOrder{}
	err = Query(db, fmt.Sprintf("SELECT * FROM %s", table), &queryMonitorsRandomOrder)
	assert.Nil(t, err)
	assert.Equal(t, expectedMonitorsRandomOrder, queryMonitorsRandomOrder)

	// Query with slice -- the columns returned are different from fields in struct
	type monitorDifferentField struct {
		// remove Memory and add id
		Host        string
		Id          uint64
		Cpu         float64
		Temperature int64
		Ts          time.Time
		IsAuthed    bool
	}
	expectedMonitorsDifferentField := []monitorDifferentField{
		{
			Host:        "Beijing",
			Ts:          time.UnixMilli(1677728740000),
			Temperature: 22,
			IsAuthed:    true,
			Cpu:         0.9,
		},
		{
			Host:        "Shanghai",
			Ts:          time.UnixMilli(1677728740012),
			Temperature: -1,
			IsAuthed:    false,
			Cpu:         0.5,
		},
	}
	queryMonitorsDifferentField := []monitorDifferentField{}
	err = Query(db, fmt.Sprintf("SELECT * FROM %s", table), &queryMonitorsDifferentField)
	assert.Nil(t, err)
	assert.Equal(t, expectedMonitorsDifferentField, queryMonitorsDifferentField)

	// Query with slice -- inconsistent field type with returned data
	type monitorWrongType struct {
		// The Cpu is int here.
		// So, when returning float64, query should fails
		Host        string
		Memory      uint64
		Cpu         int
		Temperature int64
		Ts          time.Time
		IsAuthed    bool
	}
	queryMonitorsWrongType := []monitorWrongType{}
	err = Query(db, fmt.Sprintf("SELECT * FROM %s", table), &queryMonitorsWrongType)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "incorrect type for field")
}
