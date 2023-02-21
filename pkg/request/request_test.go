package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	rb := &QueryRequest{}
	request, err := rb.Build()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, EmptyDatabaseError)

	rb.WithDatabase("disk_usage")
	request, err = rb.Build()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, EmptySqlError)

	rb.WithSql("select * from monitor")
	request, err = rb.Build()
	assert.NotNil(t, request)
	assert.Nil(t, err)
}

func TestInsertBuilder(t *testing.T) {
	// rows := InitWriteRowsWithDatabase("", "test", "monitor")
	// rows.WithColumnDefs([]*ColumnDef{
	//	InitColumnDef(0, 12, "host", true),
	//	InitColumnDef(2, 12, "ts", false),
	//	InitColumnDef(1, 12, "cpu", true),
	//	InitColumnDef(1, 12, "memory", true),
	// })
	// rows.Insert([]any{
	//	"127.0.0.1", "13957910", "0.1", "0.5",
	// })

	// req, err := IntoGreptimeRequest(rows)
	// assert.Nil(t, err)
	// assert.NotNil(t, req)
}
