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

### Datatype Supported
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

### Init Client with Config
```go
package main

import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

func InitClient() (*request.Client, error) {
    options := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }
    // To connect a database that needs authentication, for example, those on Greptime Cloud,
    // `Username` and `Password` are must.
    // To connect a local database without authentication, just leave the two fields empty.
    cfg := request.NewCfg("127.0.0.1:4001", "", "public").
        WithUserName("username").WithPassword("password").WithDialOptions(options...)

    return request.NewClient(cfg)
}
```

### Basic Example of Insert
```go
package main

import (
    "context"
    "fmt"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

func insert() {
    client, err := InitClient()
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

### Basic Example of Query

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
    client, err := InitClient()
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

### More Options
#### Precision for Timestamp
You can set a precision to determine how timstamps of series in metric are stored in database.
We support `Second`, `Millisecond`, `Microsecond` and `Nanosecond`. And the default precision is `Millisecond`.
```go
    metric.SetTimePrecision(time.Microsecond)
```
#### PromQL
We also support querying with PromQL. To use PromQL, just initiate `QueryRequest` with `PromQL` struct.
```go
    queryReq := QueryRequest{}
    queryReq.WithPromQL(&PromQL{
	    Query: table,
	    Start: "1677728740",
	    End:   "1677728740",
	    Step:  "50s",
    }).WithCatalog("").WithDatabase(database)

    resMetric, err := client.QueryMetric(context.Background(), queryReq)
```
#### Stream Insert
We support stream insert. You can send several insert request by `Send()` and notify DB the stream is at end by `CloseAndRecv()`
```go
package main

import (
    "context"
    "fmt"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

func insertWithStream() {
    client, err := InitClient()
    if err != nil {
        fmt.Printf("Fail in client initiation, err: %s", err)
    }

    // create stream for send, close and recv
    streamClient, err := client.InitStreamClient(context.Background())
	if err != nil {
        fmt.Printf("Fail in stream client initiation, err: %s", err)
    }

    // two data point
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

    // call the `Send()` for one data point per loop
    for _, monitor := range insertMonitors {
        metric := Metric{}
        metric.SetTimePrecision(time.Microsecond)
        metric.SetTimestampAlias("ts")

        series := Series{}
        series.AddTag("host", monitor.host)
        series.SetTimestamp(monitor.ts)
        series.AddField("memory", monitor.memory)
        series.AddField("cpu", monitor.cpu)
        series.AddField("temperature", monitor.temperature)
        series.AddField("is_authed", monitor.isAuthed)
        metric.AddSeries(series)

        req := InsertRequest{}
        req.WithTable(table).WithMetric(metric).WithCatalog("").WithDatabase(database)
        err = streamClient.Send(context.Background(), req)
    }

    // Notify the DB to close stream and recieve the result
    affectedRows, err := streamClient.CloseAndRecv(context.Background())
    if err != nil {
        fmt.Printf("fail to insert, err: %+v\n", err)
    } else {
        fmt.Printf("affectedRows: %+v\n", affectedRows)
    }
}
```
## License
This greptimedb-client-go uses the __Apache 2.0 license__ to strike a balance between open contributions and allowing you to use the software however you want.
