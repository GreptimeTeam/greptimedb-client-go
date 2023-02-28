package sql

import (
	req "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
	"errors"
	"strings"
)

var (
	errInvalidDSNUnescaped = errors.New("invalid DSN: did you forget to escape a param value?")
	errInvalidDSNAddr      = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	errInvalidDSNNoSlash   = errors.New("invalid DSN: missing the slash separating the database name")
)

// ParseDSN parses the DSN string to a Config
func ParseDSNToConfig(dsn string) (cfg *req.Config, err error) {
	// New config with some default values
	cfg = &req.Config{}

	// [user[:password]@][net[(addr)]]/[catalogname:][dbname]
	// Find the last '/' (since the password or the net addr might contain a '/', we need the "last")
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Password = dsn[k+1 : j]
								break
							}
						}
						cfg.UserName = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Address = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			// [catalog:][dbname]
			// Find the ':' in dsn[i+1:]
			foundColon := false
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == ':' {
					foundColon = true
					break
				}
			}
			// if ':' not exists, only database, 
			// or also contains a catalog
			if !foundColon {
				cfg.Database = dsn[i+1:]
				cfg.Catalog = ""
			} else {
				cfg.Database = dsn[j+1:]
				cfg.Catalog = dsn[i+1 : j]
			}
			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	if len(cfg.Address) == 0 {
		cfg.Address = "127.0.0.1:4001"
	}

	return
}
