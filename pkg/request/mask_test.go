package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMask(t *testing.T) {
	mask := Mask{}
	mask.set(0).set(10)
	b, err := mask.shrink(2)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 4}, b)
}
