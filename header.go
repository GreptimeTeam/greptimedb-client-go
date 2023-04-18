package greptime

import greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

type header struct {
	database string
}

func (h *header) Build(cfg *Config) (*greptimepb.RequestHeader, error) {
	if isEmptyString(h.database) {
		h.database = cfg.Database
	}

	if isEmptyString(h.database) {
		return nil, ErrEmptyDatabase
	}

	header := &greptimepb.RequestHeader{
		Dbname:        h.database,
		Authorization: cfg.BuildAuthHeader(),
	}

	return header, nil
}
