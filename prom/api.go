package prom

import (
	"encoding/json"
	"fmt"

	"github.com/prometheus/common/model"
)

type apiResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
}

func UnmarshalApiResponse(resp []byte) (*QueryResult, error) {
	var result apiResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}

	var res QueryResult
	if err := json.Unmarshal(result.Data, &res); err != nil {
		fmt.Printf("failed to unmarshal range promql, body:%s, err: %+v", result.Data, err)
		return nil, err
	}

	return &res, nil
}

// queryResult contains result data for a query.
type QueryResult struct {
	Type   model.ValueType `json:"resultType"`
	Result any             `json:"result"`

	// The decoded value.
	Val model.Value
}

func (qr *QueryResult) UnmarshalJSON(b []byte) error {
	v := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	qr.Type = v.Type

	switch v.Type {
	case model.ValScalar:
		var sv model.Scalar
		err = json.Unmarshal(v.Result, &sv)
		qr.Val = &sv

	case model.ValVector:
		var vv model.Vector
		err = json.Unmarshal(v.Result, &vv)
		qr.Val = vv

	case model.ValMatrix:
		var mv model.Matrix
		err = json.Unmarshal(v.Result, &mv)
		qr.Val = mv

	default:
		err = fmt.Errorf("unexpected value type %q", v.Type)
	}
	return err
}
