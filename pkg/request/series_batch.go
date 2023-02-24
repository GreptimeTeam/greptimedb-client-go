package request

import (
	"errors"

	model "GreptimeTeam/greptimedb-client-go/pkg/model"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// TODO(vinland-avalon): could be wrapped into struct implemented with `insert`, to make sure users transparent
type SeriesBatch struct {
	Series         []*model.Series
	Table          *string
	TagSchemaMap   map[string]greptime.ColumnDataType
	FieldSchemaMap map[string]greptime.ColumnDataType
}

func InitSeriesBatch() *SeriesBatch {
	return &SeriesBatch{
		[]*model.Series{},
		nil,
		map[string]greptime.ColumnDataType{},
		map[string]greptime.ColumnDataType{},
	}
}

func (batch *SeriesBatch) addSeries(series *model.Series) error {
	var err error
	if series == nil {
		return NilPointerErr
	}

	// check table name
	if batch.Table == nil {
		batch.Table = &series.Table
	} else {
		if *batch.Table != series.Table {
			return errors.New("different tables in one batch is not allowed")
		}
	}

	// ckeck data type of both tags column and field columns
	isColumnTypeConsistent := func(preDefinedColumns *map[string]greptime.ColumnDataType,
		pair model.KeyValuePair) error {
		dataType, err := intoGreptimeDataTypeEnum(pair.GetValue())
		if err != nil {
			return err
		}
		// if the tag's type has been defined for previous series
		// then check if the two types are same
		// else if the tag's type has not been defined for previous series
		// then add to map
		if existType, ok := (*preDefinedColumns)[pair.GetKey()]; ok {
			if existType != dataType {
				return TypeNotMatchErr
			}
		} else {
			(*preDefinedColumns)[pair.GetKey()] = dataType
		}
		return nil
	}

	for i, tag := range series.Tags {
		// if the key has been defined as a key of field, it should not be defined twice
		if _, ok := batch.FieldSchemaMap[tag.Key]; ok {
			return DuplicatedKeyErr
		}

		err = isColumnTypeConsistent((&batch.TagSchemaMap), tag)
		if err != nil {
			return err
		}

		// update the Value to the formatted Value
		value, err := intoGreptimeDataType(tag.Value)
		if err != nil {
			return err
		}
		series.Tags[i].Value = value
	}

	// just the same process as tags
	for i, field := range series.Fields {
		if _, ok := batch.TagSchemaMap[field.Key]; ok {
			return DuplicatedKeyErr
		}

		err = isColumnTypeConsistent((&batch.FieldSchemaMap), field)
		if err != nil {
			return err
		}

		value, err := intoGreptimeDataType(field.Value)
		if err != nil {
			return err
		}
		series.Fields[i].Value = value
	}

	batch.Series = append(batch.Series, series)

	return nil
}
