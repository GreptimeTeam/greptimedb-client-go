// Package greptime provides API for using GreptimeDB client in Go.
//
// call [NewClient] with [Config] to init a concurrent safe [Client], and
// prepare rows of data by [Metric] and [Series], call [Client.Insert] to insert
// metric into greptimedb, and call [Client.Query] to retrieve data from greptimedb.
package greptime
