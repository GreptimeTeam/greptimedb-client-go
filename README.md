[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/LICENSE)
[![Build Status](https://github.com/greptimeteam/greptimedb-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go)
# GreptimeDB Go Client

## Installation

```sh
go get github.com/GreptimeTeam/greptimedb-client-go
```

## Usage

### DSN - Data Source Name

When connecting to a database through greptime-client-go, we need to create a valid DSN.
Compared to [mysql](https://github.com/go-sql-driver/mysql), the Data Source Name here has a `catalogname` field.
```
[username[:password]@][protocol[(address)]]/[catalogname:][dbname]
```
There are more exampls to refer in the [dsn_test.go](pkg/sql/dsn_test.go).

### datatype supported
```go
int32, int64, int (as int64),
uint32, uint64, uint (as uint64),
float64,
bool,
string,
time.Time (as int64),
```
```go
// Attention! The following data types may cause conversion and are not recommended.
int8, int16, // they will be stored as int32
uint8, uint16, // they will be stored as uint32
float32, // it will be stored as float64
[]byte, // it will be stored as string
```

### basic example of insert
```go
import (
    "context"
    "fmt"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

func insert() {
    // Create a new client using an GreptimeDB server base URL and a database name
    options := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }
    cfg := request.NewCfg("127.0.0.1:4001", "", "public").WithDialOptions(options...)

    client, err := request.NewClient(cfg)
    if err != nil {
        fmt.Printf("Fail in client initiation, err: %s", err)
    }

    // Create a Series
    series := request.Series{}
    series.AddTag("host", "localhost")
    series.SetTimestamp(time.Now())
    series.AddField("cpu", 0.90)
    series.AddField("memory", 1024.0)

    // Create a Metric and add the Series
    metric := request.Metric{}
    metric.AddSeries(series)

    // Create an InsertRequest using fluent style
    // If the table does not exist, automatically create one with Insert
    req := request.InsertRequest{}
    req.WithTable("monitor").WithMetric(metric).WithCatalog("").WithDatabase("public")

    // Do the real Insert and Get the result
    affectedRows, err := client.Insert(context.Background(), req)
    if err != nil {
        fmt.Printf("fail to insert, err: %+v\n", err)
    } else {
        fmt.Printf("affectedRows: %+v\n", affectedRows)
    }
}
```
Attention! If the table `monitor` does not exist, it will be created

### basic example of query

```go
import (
    "context"
    "fmt"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

func query() {
    // Create a new client using an GreptimeDB server base URL and a database name
    options := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }
    cfg := request.NewCfg("127.0.0.1:4001", "", "public").WithDialOptions(options...)

    client, err := request.NewClient(cfg)
    if err != nil {
        fmt.Printf("Fail in client initiation, err: %s", err)
    }
	
    // Query with metric
    queryReq := request.QueryRequest{}
    queryReq.WithSql("SELECT * FROM monitor").WithCatalog("").WithDatabase("public")

    resMetric, err := client.QueryMetric(context.Background(), queryReq)
    if err != nil {
    	fmt.Printf("fail to query, err: %+v\n", err)
    	return
    }

    queryMonitors := []monitor{}
    for _, series := range resMetric.GetSeries() {
    	host, _ := series.Get("host")
    	ts, _ := series.GetTimestamp()
    	memory, _ := series.Get("memory")
    	cpu, _ := series.Get("cpu")
    	queryMonitors = append(queryMonitors, monitor{
    	    host:   host.(string),
    	    ts:     ts,
    	    memory: memory.(float64),
    	    cpu:    cpu.(float64),
    	})
    }
    fmt.Printf("Query monitors from db: %+v", queryMonitors)
}


```
## License
This greptimedb-client-go uses the __Apache 2.0 license__ to strike a balance between open contributions and allowing you to use the software however you want.
