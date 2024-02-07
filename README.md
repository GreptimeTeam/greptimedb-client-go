[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/LICENSE)
[![Build Status](https://github.com/greptimeteam/greptimedb-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-client-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-client-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/GreptimeTeam/greptimedb-client-go.svg)](https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-client-go)
# GreptimeDB Go Client

Provide API for using GreptimeDB client in Go.

NOTE: This repository is deprecated. Please visit [ingester-go][ingester-go] for detail.

## Installation

```sh
go get -u github.com/GreptimeTeam/greptimedb-client-go
```

## Documentation

visit [docs](./docs) to get complete examples. You can also visit [Documentation][document] more details.

## API reference

#### Datatype Supported

- int8, int16, int32, int64, int
- uint8, uint16, uint32, uint64, uint
- float32, float64
- bool
- []byte
- string
- time.Time

#### Customize metric Timestamp

you can customize timestamp index via calling methods of [Metric][metric_doc]

- `metric.SetTimePrecision(time.Microsecond)`
- `metric.SetTimestampAlias("timestamp")`

## License

This greptimedb-client-go uses the __Apache 2.0 license__ to strike a balance
between open contributions and allowing you to use the software however you want.

<!-- links -->
[document]: https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-client-go
[metric_doc]: https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-client-go#Metric
[ingester-go]: https://github.com/GreptimeTeam/greptimedb-ingester-go
