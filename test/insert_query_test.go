package test

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
	_ "github.com/GreptimeTeam/greptimedb-client-go/pkg/sql"
)

var (
	pool     *dockertest.Pool
	resource *dockertest.Resource

	grpcAddr   string
	database   string = "public"
	driverName string = "greptimedb"
	repo       string = "greptime/greptimedb"
	tag        string = "0.1.0-alpha-20230227-weekly"
)

type weather struct {
	City        string
	Temperature float64
	Moisture    float64
	Ts          time.Time
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
	originWeathers := []weather{
		{
			City:        "Beijing",
			Ts:          time.UnixMilli(1677728740000),
			Temperature: 22.0,
			Moisture:    0.45,
		},
		{
			City:        "Shanghai",
			Ts:          time.UnixMilli(1677728740012),
			Temperature: 28.0,
			Moisture:    0.80,
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
	for _, originWeather := range originWeathers {
		series := request.Series{}
		series.AddTag("city", originWeather.City)
		series.SetTimeWithKey("ts", originWeather.Ts)
		series.AddField("temperature", originWeather.Temperature)
		series.AddField("moisture", originWeather.Moisture)
		metric.AddSeries(series)
	}

	req := request.InsertRequest{}
	req.WithTable("weather").WithMetric(metric).WithCatalog("").WithDatabase("public")

	affectedRows, err := client.Insert(context.Background(), req)
	if err != nil {
		fmt.Printf("client.Insert err: %v", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(originWeathers)), affectedRows.Value)

	// Query
	db, err := sql.Open(driverName, fmt.Sprintf("(%s)/%s", grpcAddr, database))
	assert.Nil(t, err)

	res, err := db.Query("SELECT * FROM weather")
	assert.Nil(t, err)

	var actuallWeathers []weather
	for res.Next() {
		var actuallWeather weather
		err = res.Scan(&actuallWeather.City, &actuallWeather.Temperature,
			&actuallWeather.Moisture, &actuallWeather.Ts)
		assert.Nil(t, err)
		actuallWeathers = append(actuallWeathers, actuallWeather)
	}

	assert.Nil(t, err)
	assert.Equal(t, originWeathers, actuallWeathers)

	// Query with slice
	actualWeathers2 := []weather{}
	err = request.Query(db, "SELECT * FROM weather", &actualWeathers2)
	assert.Nil(t, err)
	assert.Equal(t, originWeathers, actualWeathers2)
}
