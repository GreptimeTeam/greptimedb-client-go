# GreptimeDB Go Client

# Getting Started

### DSN (Data Source Name)

The Data Source Name has a common format, like e.g. [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php) uses it, but without type-prefix (optional parts marked by squared brackets):
```
[username[:password]@][protocol[(address)]]/[catalogname:]dbname
```

A DSN in its fullest form:
```
username:password@protocol(address)/catalogname:dbname
```

Except for the databasename, all values are optional. So the minimal DSN is:
```
/dbname
```

If you do not want to preselect a database, leave `dbname` empty:
```
/
```
This has the same effect as an empty DSN string:
```

```

Alternatively, [Config.FormatDSN](https://godoc.org/github.com/go-sql-driver/mysql#Config.FormatDSN) can be used to create a DSN string by filling a struct.

#### Password
Passwords can consist of any character. Escaping is **not** necessary.

#### Protocol
See [net.Dial](https://golang.org/pkg/net/#Dial) for more information which networks are available.
In general you should use an Unix domain socket if available and TCP otherwise for best performance.

#### Address
For TCP and UDP networks, addresses have the form `host[:port]`.
If `host` is omitted, the default port will be used -- 127.0.0.1.
If `port` is omitted, the default port will be used -- 4001.
If `host` is a literal IPv6 address, it must be enclosed in square brackets.
The functions [net.JoinHostPort](https://golang.org/pkg/net/#JoinHostPort) and [net.SplitHostPort](https://golang.org/pkg/net/#SplitHostPort) manipulate addresses in this form.

## Installing

```sh
go get github.com/GreptimeTeam/greptimedb-client-go
```

## Examples

The [examples](/examples) directory contains code samples that use the SDK to interact with GreptimeDB

# Features

# User Guide
