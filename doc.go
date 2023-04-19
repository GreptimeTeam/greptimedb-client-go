// Package greptime provides API for using GreptimeDB client in Go.
//
// # Basic Insert and Query
//
// call [NewClient] with [Config] to init a concurrent safe [Client], and
// prepare rows of data by [Metric] and [Series], call [Client.Insert] to insert
// [InsertRequest] into greptimedb, and call [Client.Query] to retrieve data from
// greptimedb via [QueryRequest].
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
