package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsStructSliceSettableWithNilSlicePointer(t *testing.T) {
	err := isStructSliceSettable(nil)
	assert.NotNil(t, err)
	assert.Equal(t, "dest must be a pointer to slice", err.Error())
}

func TestIsStructSliceSettableWithNonPointerSlice(t *testing.T) {
	slice := make([]int, 0)
	err := isStructSliceSettable(slice)
	assert.NotNil(t, err)
	assert.Equal(t, "dest must be a pointer to slice", err.Error())
}

func TestIsStructSliceSettableWithFieldCanNotSet(t *testing.T) {
	type NonSettableStruct struct {
		ptr int
	}

	slice := []NonSettableStruct{}
	err := isStructSliceSettable(&slice)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not settable")
}
