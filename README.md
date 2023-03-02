# GreptimeDB Go Client

# Getting Started

### DSN - Data Source Name

When connecting to a database through greptime-client-go, we need to create a valid DSN.  
Compared to [mysql](https://github.com/go-sql-driver/mysql), the Data Source Name here has a `catalogname` valid.
```
[username[:password]@][protocol[(address)]]/[catalogname:][dbname]
```

Except for the databasename, all values are optional. So the minimal DSN is:
```
/dbname
```

If you do not want to preselect a database, leave `dbname` empty:
```
/
```

#### Password
Passwords can consist of any character. Escaping is **not** necessary.

#### Protocol
Communication protocol to use ( i.e. tcp, unix etc.)

#### Address
For TCP and UDP networks, addresses have the form `host[:port]`.
If `host` is omitted, the default port will be used -- 127.0.0.1.
If `port` is omitted, the default port will be used -- 4001.
If `host` is a literal IPv6 address, it must be enclosed in square brackets.

## Installing

```sh
go get github.com/GreptimeTeam/greptimedb-client-go
```

## Examples

#### Setup GreptimeDB

1. start GreptimeDB standalone container

```shell
docker run --rm -p 4001:4001 -p 4002:4002 greptime/greptimedb:latest standalone start --mysql-addr=0.0.0.0:4002 --rpc-addr=0.0.0.0:4001
```

2. insert

this will create `monitor` table in greptimedb automatically.

```shell
go run examples/insert/insert.go
```

3. query

```shell
go run examples/query/query.go
```


# Features

# User Guide
