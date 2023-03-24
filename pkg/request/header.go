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

func (h *Header) IsDatabaseEmpty() bool {
	return IsEmptyString(h.Database)
}

func (h *Header) buildRequestHeader(catalog, database string) (*greptime.RequestHeader, error) {
	header := &greptime.RequestHeader{
		Catalog: h.Catalog,
		Schema:  h.Database,
	}

	if IsEmptyString(header.Catalog) && !IsEmptyString(catalog) {
		header.Catalog = catalog
	}

	if IsEmptyString(header.Schema) {
		if IsEmptyString(database) {
			return nil, ErrEmptyDatabase
		} else {
			header.Schema = database
		}
	}

	return header, nil
}
