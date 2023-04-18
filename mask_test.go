package greptime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// 00000100 00000001
// 4        1
//
// []byte{1, 4} // LittleEndian
func TestMask(t *testing.T) {
	mask := mask{}
	mask.set(0).set(10)
	b, err := mask.shrink(2)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 4}, b)
}
