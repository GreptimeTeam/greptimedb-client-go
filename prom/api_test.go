// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prom

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApiResponseError(t *testing.T) {
	data, err := json.Marshal(map[string]any{"resultType": "", "result": []byte{}})
	assert.Nil(t, err)

	resp := apiResponse{
		Status: "error",
		Data:   data,
		Type:   "RateLimited",
		Msg:    "Read request banned for xxx until 1692722423416",
	}
	b, err := json.Marshal(resp)
	assert.Nil(t, err)
	res, err := UnmarshalApiResponse(b)

	assert.Nil(t, res)
	assert.True(t, IsRateLimitedError(err))

	e, ok := err.(*apiResponse)
	assert.True(t, ok)
	assert.Equal(t, "error", e.Status)
	assert.NotEmpty(t, e.Data)
	assert.Equal(t, "RateLimited", e.Type)
	assert.Equal(t, "Read request banned for xxx until 1692722423416", e.Msg)
}
