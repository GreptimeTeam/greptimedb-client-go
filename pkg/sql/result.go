package sql

type result struct {
	affectedRows int64
	insertId     int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.insertId, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}
