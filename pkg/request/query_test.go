package request

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func mockRowsToSqlRows(mockRows *sqlmock.Rows) *sql.Rows {
	db, mock, _ := sqlmock.New()
	mock.ExpectQuery("select").WillReturnRows(mockRows)
	rows, _ := db.Query("select")
	return rows
}

type Person struct {
	Name string
	Age  int
}

func TestFillStructSliceFromRows(t *testing.T) {
	expected := []Person{
		{"Alice", 25},
		{"Bob", 30},
		{"Charlie", 35},
	}

	// Set up a mock rows object
	columns := []string{"name", "age"}
	rows := sqlmock.NewRows(columns).
		AddRow("Alice", 25).
		AddRow("Bob", 30).
		AddRow("Charlie", 35)

	slice := []Person{}
	err := fillStructSlice(&slice, mockRowsToSqlRows(rows))
	assert.Nil(t, err)
	assert.Equal(t, slice, expected)
}
