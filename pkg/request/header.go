package request

import greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

type Header struct {
	Catalog  string // optional
	Database string // required
}

func (h *Header) WithCatalog(catalog string) *Header {
	h.Catalog = catalog
	return h
}

func (h *Header) WithDatabase(database string) *Header {
	h.Database = database
	return h
}

func (h *Header) buildRequestHeader(cfg *Config) (*greptime.RequestHeader, error) {
	header := &greptime.RequestHeader{
		Catalog: h.Catalog,
		Schema:  h.Database,
	}

	if IsEmptyString(header.Catalog) && !IsEmptyString(cfg.Catalog) {
		header.Catalog = cfg.Catalog
	}

	if IsEmptyString(header.Schema) {
		if IsEmptyString(cfg.Database) {
			return nil, ErrEmptyDatabase
		} else {
			header.Schema = cfg.Database
		}
	}

	header.Authorization = cfg.buildAuth()

	return header, nil
}
