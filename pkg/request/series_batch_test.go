package request

import (
	model "GreptimeTeam/greptimedb-client-go/pkg/model"
	"testing"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
)

func TestAddSeries(t *testing.T) {
	batch := InitSeriesBatch()
	// case 1: add successfully
	seriesA := model.Series{
		Table: "monitor",
		Tags: []model.Tag{
			{Key: "host", Value: "123"},
		},
		Fields: []model.Field{
			{Key: "cpu", Value: 0.9},
			{Key: "memory", Value: 1024},
		},
	}
	err := batch.addSeries(&seriesA)

	assert.Nil(t, err)
	assert.Equal(t, *batch.Table, "monitor")
	assert.Equal(t, len(batch.Series), 1)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"host": greptime.ColumnDataType_STRING,
	}, batch.TagSchemaMap)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"memory": greptime.ColumnDataType_INT64,
		"cpu":    greptime.ColumnDataType_FLOAT64,
	}, batch.FieldSchemaMap)

	// case 2: the second series has another tag
	seriesB := model.Series{
		Table: "monitor",
		Tags: []model.Tag{
			{Key: "host", Value: "123"},
			{Key: "region", Value: "Beijing"},
		},
		Fields: []model.Field{
			{Key: "cpu", Value: 0.9},
			{Key: "memory", Value: 1024},
		},
	}
	err = batch.addSeries(&seriesB)

	assert.Nil(t, err)
	assert.Equal(t, *batch.Table, "monitor")
	assert.Equal(t, len(batch.Series), 2)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"host":   greptime.ColumnDataType_STRING,
		"region": greptime.ColumnDataType_STRING,
	}, batch.TagSchemaMap)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"memory": greptime.ColumnDataType_INT64,
		"cpu":    greptime.ColumnDataType_FLOAT64,
	}, batch.FieldSchemaMap)

	// case 3: the third series has a wrong data type with tag
	seriesC := model.Series{
		Table: "monitor",
		Tags: []model.Tag{
			{Key: "host", Value: 123},
			{Key: "region", Value: "Beijing"},
		},
		Fields: []model.Field{
			{Key: "cpu", Value: 0.9},
			{Key: "memory", Value: 1024},
		},
	}
	err = batch.addSeries(&seriesC)

	assert.Equal(t, err, TypeNotMatchErr)
	assert.Equal(t, *batch.Table, "monitor")
	assert.Equal(t, len(batch.Series), 2)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"host":   greptime.ColumnDataType_STRING,
		"region": greptime.ColumnDataType_STRING,
	}, batch.TagSchemaMap)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"memory": greptime.ColumnDataType_INT64,
		"cpu":    greptime.ColumnDataType_FLOAT64,
	}, batch.FieldSchemaMap)

	// case 4: the fourth series has a tag key as defined in previous field
	seriesD := model.Series{
		Table: "monitor",
		Tags: []model.Tag{
			{Key: "cpu", Value: 0.9},
			{Key: "region", Value: "Beijing"},
		},
		Fields: []model.Field{
			{Key: "cpu", Value: 0.9},
			{Key: "memory", Value: 1024},
		},
	}
	err = batch.addSeries(&seriesD)

	assert.Equal(t, err, DuplicatedKeyErr)
	assert.Equal(t, *batch.Table, "monitor")
	assert.Equal(t, len(batch.Series), 2)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"host":   greptime.ColumnDataType_STRING,
		"region": greptime.ColumnDataType_STRING,
	}, batch.TagSchemaMap)
	assert.Equal(t, map[string]greptime.ColumnDataType{
		"memory": greptime.ColumnDataType_INT64,
		"cpu":    greptime.ColumnDataType_FLOAT64,
	}, batch.FieldSchemaMap)
}
