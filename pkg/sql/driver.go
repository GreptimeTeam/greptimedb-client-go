package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type Driver struct{}

// Open new Connection.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	c := &connector{cfg}
	return c.Connect(context.Background())
}

// If a Driver implements DriverContext, then sql.DB will call
// OpenConnector to obtain a Connector and then invoke
// that Connector's Connect method to obtain each needed connection,
// instead of invoking the Driver's Open method for each connection.
// The two-step sequence allows drivers to parse the name just once
// and also provides access to per-Conn contexts.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &connector{cfg}, nil
}

func init() {
	sql.Register("greptimedb", &Driver{})
}

// TODO(yuanbohan): check if the dsn is valid
// TODO(yuanbohan): extract the database variable from the dsn
func ParseDSN(dsn string) (*req.Config, error) {
	// TODO(yuanbohan): catalog and database SHOULD be initiated here
	// `public` is just for example
	// cfg := req.NewCfg(dsn, "", "public")
	cfg, err := ParseDSNToConfig(dsn)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
