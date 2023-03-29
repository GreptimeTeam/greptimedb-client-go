package request

import greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

type Header struct {
	Database string
}

func (h *Header) WithDatabase(database string) *Header {
	h.Database = database
	return h
}

func (h *Header) buildRequestHeader(cfg *Config) (*greptime.RequestHeader, error) {
	header := &greptime.RequestHeader{
		Dbname: h.Database,
	}

	if IsEmptyString(header.Dbname) {
		if IsEmptyString(cfg.Database) {
			return nil, ErrEmptyDatabase
		} else {
			header.Dbname = cfg.Database
		}
	}

	header.Authorization = cfg.buildAuth()

	return header, nil
}
