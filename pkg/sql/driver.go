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
	c, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

func init() {
	sql.Register("greptimedb", &Driver{})
}

// TODO(yuanbohan): check if the dsn is valid
// TODO(yuanbohan): extract the database variable from the dsn
func ParseDSN(dsn string) (*connector, error) {
	// TODO(yuanbohan): catalog and database SHOULD be initiated here
	// `public` is just for example
	cfg := req.NewCfg(dsn, "", "public")

	c := &connector{
		cfg: cfg,
	}
	return c, nil
}
