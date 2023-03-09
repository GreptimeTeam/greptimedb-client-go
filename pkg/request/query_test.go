package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// func mockRowsToSqlRows(mockRows *sqlmock.Rows) *sql.Rows {
// 	db, mock, _ := sqlmock.New()
// 	mock.ExpectQuery("select").WillReturnRows(mockRows)
// 	rows, _ := db.Query("select")
// 	return rows
// }

type Person struct {
	Name string
	Age  int
}

// func TestFillStructSliceWithLessRowDataColumns(t *testing.T) {
// 	expected := []Person{
// 		{"Alice", 25},
// 		{"Bob", 30},
// 		{"Charlie", 35},
// 	}

// 	// Set up a mock rows object
// 	rows := sqlmock.NewRows([]string{"age"}).
// 		AddRow(25).
// 		AddRow(30).
// 		AddRow(35)

// 	// Call the function and check the result
// 	slice := []Person{}
// 	err := fillStructSlice(&slice, mockRowsToSqlRows(rows))
// 	assert.Nil(t, err)
// 	assert.Equal(t, slice, expected)
// }

func TestIsStructSliceSettableWithNilSlicePointer(t *testing.T) {
	err := isStructSliceSettable(nil)
	assert.NotNil(t, err)
	assert.Equal(t, "dest must be a pointer to a slice", err.Error())
}

func TestIsStructSliceSettableWithNonPointerSlice(t *testing.T) {
	slice := make([]Person, 0)
	err := isStructSliceSettable(slice)
	assert.NotNil(t, err)
	assert.Equal(t, "dest must be a pointer to a slice", err.Error())
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
