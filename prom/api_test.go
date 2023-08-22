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
