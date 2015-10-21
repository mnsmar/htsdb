// Package htsdb wraps a database connection to provide a convenient loop
// interface for reading database records. Successive calls to the Next method
// will step through the records of the database. Iteration stops
// unrecoverably when records are exhausted or at the first error.
//
// type Record struct {
// 	Strand  int `db:"strand_field"`
// 	Start   int
// }
//
// db, err := sql.Open("foo", "bar")
//
// r, err := NewReader(db, "foo", &Record{}, "SELECT * FROM FOO")
// for r.Next() {
//     rec, ok := r.Record().(*Record)
//     fmt.Println(rec)
// }
// if r.Error() !=  nil {
// 	panic(r.Error)
// }
package htsdb

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Reader encapsulates a connection to a database and acts as an iterator for
// the records. Internally the reader maps each database row to dest.
type Reader struct {
	db    *sqlx.DB
	dest  interface{}
	query string
	rows  *sqlx.Rows
	err   error
}

// NewReader returns a new reader that reads from db by runs the given query
// and maps rows into dest.
func NewReader(db *sql.DB, driverName string, dest interface{}, query string,
) (*Reader, error) {

	sqlxDB := sqlx.NewDb(db, driverName)

	rows, err := sqlxDB.Queryx(query)
	if err != nil {
		return nil, err
	}

	return &Reader{db: sqlxDB, dest: dest, query: query, rows: rows}, nil
}

// Next advances the iterator past the next record, which will then be
// available through Record(). It returns false when the iteration stops,
// either by reaching the end of the input or an error. After Next returns
// false, the Error method will return any error that occurred during
// iteration.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	ok := r.rows.Next()
	if !ok {
		r.err = r.rows.Err()
		return false
	}
	r.err = r.rows.StructScan(r.dest)
	return r.err == nil
}

// Error returns the error that was encountered by the iterator.
func (r *Reader) Error() error {
	return r.err
}

// Record returns the most recent record read by a call to Next.
func (r *Reader) Record() interface{} { return r.dest }

// Close closes the database connection.
func (r *Reader) Close() {
	r.db.Close()
}
