package sql

type Result struct {
	affectedRows int64
	insertId     int64
}

func (r *Result) LastInsertId() (int64, error) {
	return r.insertId, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}
