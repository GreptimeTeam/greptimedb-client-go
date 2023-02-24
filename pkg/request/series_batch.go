package request

import (
	"errors"

	model "GreptimeTeam/greptimedb-client-go/pkg/model"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// TODO(vinland-avalon): could be wrapped into struct implemented with `insert`, to make sure users transparent
type SeriesBatch struct {
	Series         []*model.Series
	TagSchemaMap   map[string]greptime.ColumnDataType
	FieldSchemaMap map[string]greptime.ColumnDataType
}

func (batch *SeriesBatch) addSeries(series *model.Series) error {
	var err error
	if series == nil {
		return NilPointerErr
	}

	// TODO(vinland-avalon): the logic to deal with `tag` and `field` is same, try to simplify it
	for i, tag := range series.Tags {
		var value any
		var dataType greptime.ColumnDataType
		// if the key has been defined as a key of field, it should not be defined twice
		if _, ok := batch.FieldSchemaMap[tag.Key]; ok {
			return errors.New("the key should not be defined as both tag and field")
		}
		// if the tag's type has been defined for previous series
		// then 1)check if the type is valid, 2)check if the two types are same
		if existType, ok := batch.TagSchemaMap[tag.Key]; ok {
			value, dataType, err = intoGreptimeDataType(tag.Value)
			if err != nil {
				return err
			}
			if existType != dataType {
				return TypeNotMatchErr
			}
			// else if the tag's type has not been defined for previous series
			// then 1)check if the type is valid, 2)add to map
		} else {
			value, dataType, err = intoGreptimeDataType(tag.Value)
			if err != nil {
				return err
			}
			batch.TagSchemaMap[tag.Key] = dataType
		}

		// fill dataType into the tag of series
		series.Tags[i].SetDataType(dataType)
		// update the Value to the formatted Value
		series.Tags[i].Value = value
	}

	// just the same process as tags
	for i, field := range series.Fields {
		var value any
		var dataType greptime.ColumnDataType

		if _, ok := batch.TagSchemaMap[field.Key]; ok {
			return errors.New("the key should not be defined as both tag and field")
		}

		if existType, ok := batch.FieldSchemaMap[field.Key]; ok {
			value, dataType, err = intoGreptimeDataType(field.Value)
			if err != nil {
				return err
			}
			if existType != dataType {
				return TypeNotMatchErr
			}
		} else {
			value, dataType, err = intoGreptimeDataType(field.Value)
			if err != nil {
				return err
			}
			batch.FieldSchemaMap[field.Key] = dataType
		}

		series.Fields[i].SetDataType(dataType)
		series.Fields[i].Value = value
	}
	return nil
}
