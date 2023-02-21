package sql

// refer https://go.dev/doc/go1.8#database_sql to use new context related interface

// The package now supports context.Context. There are new methods ending in Context such as DB.QueryContext and DB.PrepareContext that take context arguments. Using the new Context methods ensures that connections are closed and returned to the connection pool when the request is done; enables canceling in-progress queries should the driver support that; and allows the database pool to cancel waiting for the next available connection.

// The IsolationLevel can now be set when starting a transaction by setting the isolation level on TxOptions.Isolation and passing it to DB.BeginTx. An error will be returned if an isolation level is selected that the driver does not support. A read-only attribute may also be set on the transaction by setting TxOptions.ReadOnly to true.

// Queries now expose the SQL column type information for drivers that support it. Rows can return ColumnTypes which can include SQL type information, column type lengths, and the Go type.

// A Rows can now represent multiple result sets. After Rows.Next returns false, Rows.NextResultSet may be called to advance to the next result set. The existing Rows should continue to be used after it advances to the next result set.

// NamedArg may be used as query arguments. The new function Named helps create a NamedArg more succinctly.

// If a driver supports the new Pinger interface, the DB.Ping and DB.PingContext methods will use that interface to check whether a database connection is still valid.

// The new Context query methods work for all drivers, but Context cancellation is not responsive unless the driver has been updated to use them. The other features require driver support in database/sql/driver. Driver authors should review the new interfaces. Users of existing driver should review the driver documentation to see what it supports and any system specific documentation on each feature.
