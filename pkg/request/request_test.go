package request

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	rb := &QueryRequest{}
	request, err := rb.Build()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptyDatabase)

	rb.WithDatabase("disk_usage")
	request, err = rb.Build()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptySql)

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

func TestBuildInsertRequest(t *testing.T) {
	type Monitor struct {
		Host   string    `db:"host,INDEX"`
		Ts     time.Time `db:"ts,TIMESTAMP"`
		Cpu    float64   `db:"cpu"`
		Memory float64   `db:"memory"`
	}

	monitors := []Monitor{
		{"host1", time.Now(), 1, 1},
		{"host2", time.Now(), 2, 2},
	}

	data := make([]any, len(monitors))
	for idx, monitor := range monitors {
		data[idx] = monitor
	}

	req := InsertRequest{
		Header: Header{
			Catalog:  "catalog",
			Database: "database",
		},
		Table: "table",
		Data:  data,
	}

	cols, err := req.Build()
	fmt.Println(err)
	fmt.Println(cols)
}
