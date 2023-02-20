package request

import "strings"

type Header struct {
	Catalog  string // optional
	Datadase string // required
}

func (h *Header) WithCatalog(catalog string) *Header {
	h.Catalog = catalog
	return h
}

func (h *Header) WithDatabase(database string) *Header {
	h.Datadase = database
	return h
}

func (h *Header) IsDatabaseEmpty() bool {
	return len(strings.TrimSpace(h.Datadase)) == 0
}
