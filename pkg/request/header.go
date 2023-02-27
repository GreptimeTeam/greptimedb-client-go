package request

import "strings"

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
	return len(strings.TrimSpace(h.Database)) == 0
}
