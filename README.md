[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/LICENSE)
[![Build Status](https://github.com/greptimeteam/greptimedb-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/GreptimeTeam/greptimedb-client-go.svg)](https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-client-go)
# GreptimeDB Go Client

Provide API for using GreptimeDB client in Go.

## Installation

```sh
go get github.com/GreptimeTeam/greptimedb-client-go
```

## Example

you can visit [Example][example] for usage details.

## Usage

#### Datatype Supported
```go
int32, int64, int (as int64),
uint32, uint64, uint (as uint64),
float32, float64,
bool,
[]byte,
string,
time.Time,
```

```go
// Attention! The following data types may cause conversion, not recommended.
int8, int16    // stored as int32
uint8, uint16  // stored as uint32
```

#### Precision for Timestamp

The default precision is `Millisecond`, you can set a different precision,
once the precision is setted, you can not change it any more.

- `Second`
- `Millisecond`
- `Microsecond`
- `Nanosecond`

```go
metric.SetTimePrecision(time.Microsecond)
```

#### Stream Insert

You can send several insert request by `Send()` and notify DB no more messages by `CloseAndRecv()`

you can visit [stream_client_test.go](stream_client_test.go) for details

#### Prometheus

We also support querying with RangePromql and Promql(TODO).

you can visit [promql_test.go](promql_test.go) for details

## License

This greptimedb-client-go uses the __Apache 2.0 license__ to strike a balance
between open contributions and allowing you to use the software however you want.

<!-- links -->
[example]: https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-client-go#example-package
