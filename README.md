[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/LICENSE)
[![Build Status](https://github.com/greptimeteam/greptimedb-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go)
# GreptimeDB Go Client

## Installation
Make sure [Git is installed](https://git-scm.com/downloads) on your machine and in your system's `PATH`.  

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
There are more exampls to refer in the [dsn_test.go](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/pkg/sql/dsn_test.go).

### setup GreptimeDB

start GreptimeDB standalone container

```shell
docker run --rm -p 4001:4001 -p 4002:4002 greptime/greptimedb:latest standalone start --mysql-addr=0.0.0.0:4002 --rpc-addr=0.0.0.0:4001
```

### basic example of insert

```sql
INSERT INTO monitor(host, cpu, memory, ts) VALUES ('localhost', 0.90, 1024.0, 1660897955000);
```
The following code can do Insert just as the above sql do.

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

func main() {
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
	series.SetTimeWithKey("ts", time.UnixMilli(1660897955000))
	series.AddField("cpu", 0.90)
	series.AddField("memory", 1024.0)

    // Create a Metric and add the Series
	metric := request.Metric{}
	metric.SetTimePrecision(time.Microsecond)
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
Attention! If the table `monitor` does not exist, it will be created automatically. The table created after the code will be in schama like
```sql
SELECT * FROM monitor;
```


### basic example of query

```sql
INSERT INTO monitor(host, cpu, memory, ts) VALUES ('localhost', 0.90, 1024.0, 1660897955000);
```
The following code can do Insert just as the above sql do.

```go
package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/GreptimeTeam/greptimedb-client-go/pkg/sql"
)

type Monitor struct {
	Host   string
	Cpu    float64
	Memory float64
	Ts     time.Time
}

func main() {
    // Open a GreptimeDB connection with database/sql API.
    // Use `greptimedb` as driverName and a valid DSN to define data source 
	db, err := sql.Open("greptimedb", "(127.0.0.1:4001)/public")
	defer db.Close()
	if err != nil {
		fmt.Printf("sql.Open err: %v", err)
	}

	res, err := db.Query("SELECT * FROM monitor")
	defer res.Close()

	if err != nil {
		fmt.Printf("db.Query err: %v", err)
	}

	var monitors []Monitor
    // Use Next() to iterate over query result lines
	for res.Next() {
		var monitor Monitor
		err := res.Scan(&monitor.Host, &monitor.Cpu, &monitor.Memory, &monitor.Ts)

		if err != nil {
			fmt.Printf("res.Scan err: %v", err)
			continue
		}
		monitors = append(monitors, monitor)
	}

	fmt.Printf("%#v\n", monitors)
}
```
## License
This greptimedb-client-go uses the __Apache 2.0 license__ to strike a balance between open contributions and allowing you to use the software however you want.
