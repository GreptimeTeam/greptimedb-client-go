[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/LICENSE)
[![Build Status](https://github.com/greptimeteam/greptimedb-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go)
# GreptimeDB Go Client

## Installation

```sh
go get github.com/GreptimeTeam/greptimedb-client-go
```

## Usage

### Example

you can visit [example](examples/request.go) for usage details.

```shell
go run examples/request.go
```

this will output the following:

```text
Success! AffectedRows: value:1
Query monitors from db: [{host:localhost memory:1024 cpu:0.9 ts:{wall:4000000 ext:63817385045 loc:0xd6b740}}]
```

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

### More Options

#### Precision for Timestamp

The default precision is `Millisecond`, you can set a different precision before inserting into greptimedb.
We support `Second`, `Millisecond`, `Microsecond` and `Nanosecond`. Once the precision is setted, you can not
change it any more.

```go
    metric.SetTimePrecision(time.Microsecond)
```

#### Stream Insert

We support stream insert. You can send several insert request by `Send()` and notify DB the stream is at end by `CloseAndRecv()`

you can visit `stream_client_test.go` for details

#### Prometheus

We also support querying with RangePromql and Promql(TODO).

you can visit `promql_test.go` for details

## License

This greptimedb-client-go uses the __Apache 2.0 license__ to strike a balance between open contributions and allowing you to use the software however you want.
