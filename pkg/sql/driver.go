package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type Driver struct {
}

// Open new Connection.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func init() {
	sql.Register("greptimedb", &Driver{})
}

// ParseDSN TODO(yuanbohan): real logic
func ParseDSN(dsn string) (*req.Config, error) {
	// TODO(yuanbohan): check if the dsn is valid
	return req.NewCfg(dsn), nil
}
