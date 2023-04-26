// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package greptime provides API for using GreptimeDB client in Go.
//
// # Basic Insert and Query
//
// You can call [NewClient] with [Config] to init a concurrent safe [Client], and
// construct rows of data by [Metric] and [Series], call [Client.Insert] to insert
// [InsertRequest] into greptimedb, and call [Client.Query] to retrieve data from
// greptimedb via [QueryRequest].
//
// # Promql
//
// You can also call [Client.PromqlQuery] to retrieve data in []byte format, which
// is absolutely the same as Prometheus. You can choose [InstantPromql] or [RangePromql]
// to get vector or matrix result.
//
// # Series
//
// You don't need to create the table, it will be created automatically via [Series] fields.
// What you have to know about [Series] in advance:
//
//   - Tag is like index, it helps you to retrive data more efficiently
//   - Field is like value, it can be used to analyze, calculate, aggregate, etc,.
//   - Timestamp is required for timeseries data
//
// Once the schema is created automatically, it can not be changed by [Client], it
// will fail if the column type does not match
//
// # Metric
//
// [Metric] is like multiple [Series], it will check if all of the [Series] are valid:
//
//   - the same column name in different series: data type MUST BE the same
//   - Tag and Field MUST NOT contain the same column name
//   - timestamp MUST NOT BE empty
//
// Also, [Metric] can set:
//
//   - [Metric.SetTimePrecision]
//   - [Metric.SetTimestampAlias]
package greptime
