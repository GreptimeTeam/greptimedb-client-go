// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prom

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
)

// apiResponse implements error interface
type apiResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`

	Type string `json:"errorType"`
	Msg  string `json:"error"`
}

func (r *apiResponse) isError() bool {
	return !strings.EqualFold(r.Status, "success")
}

// IsRateLimited checkes if this error is caused by rate limit restriction
func (e *apiResponse) isRateLimited() bool {
	return strings.EqualFold(e.Type, "RateLimited")
}

func (e *apiResponse) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Msg)
}

func UnmarshalApiResponse(body []byte) (*QueryResult, error) {
	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	if resp.isError() {
		return nil, &resp
	}

	var res QueryResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		fmt.Printf("failed to unmarshal range promql, body:%s, err: %+v", resp.Data, err)
		return nil, err
	}

	return &res, nil
}

func IsRateLimitedError(err error) bool {
	e, ok := err.(*apiResponse)
	return ok && e.isRateLimited()
}

// QueryResult contains result data for a query.
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
		err = fmt.Errorf("unexpected value type %q with data: '%s'", v.Type.String(), string(b))
	}
	return err
}
