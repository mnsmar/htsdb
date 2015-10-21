package htsdb

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"testing"
)

type Record struct {
	Start int `db:"start"`
	End   int `db:"stop"`
}

type RecordStartOnly struct {
	Start int `db:"start"`
}

func newTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal("Failed to open database:", err)
	}

	_, err = db.Exec("CREATE TABLE foo (start, stop)")
	if err != nil {
		t.Fatal("Failed to create table:", err)
	}
	return db
}

var readTests = []struct {
	Name, Query, Error string
	Inserts            []string
	Cnt                int
	Dest               interface{}
}{
	{
		Name:    "single record",
		Query:   "SELECT * FROM foo",
		Inserts: []string{"INSERT INTO foo(start, stop) VALUES(1, 2)"},
		Cnt:     1,
		Dest:    &Record{},
	},
	{
		Name:  "three records",
		Query: "SELECT * FROM foo",
		Inserts: []string{
			"INSERT INTO foo(start, stop) VALUES(1, 2)",
			"INSERT INTO foo(start, stop) VALUES(3, 4)",
			"INSERT INTO foo(start, stop) VALUES(5, 6)"},
		Cnt:  3,
		Dest: &Record{},
	},
	{
		Name:  "failing query",
		Query: "SELECT * FROM bar",
		Error: "no such table: bar",
		Dest:  &Record{},
	},
	{
		Name:    "failing scan",
		Query:   "SELECT * FROM foo",
		Inserts: []string{"INSERT INTO foo(start, stop) VALUES(1, 2)"},
		Cnt:     0,
		Dest:    &RecordStartOnly{},
		Error:   "missing destination name stop",
	},
}

func TestReader(t *testing.T) {
	for _, tt := range readTests {
		db := newTestDB(t)
		defer db.Close()

		for _, ins := range tt.Inserts {
			_, err := db.Exec(ins)
			if err != nil {
				t.Fatalf("%s:Failed insert %s:%v", tt.Name, ins, err)
			}
		}

		r, err := NewReader(db, "sqlite3", tt.Dest, tt.Query)
		if err != nil {
			if !strings.Contains(err.Error(), tt.Error) {
				t.Errorf("%s:unexpected error:%v", tt.Name, err)
			}
			continue
		}
		defer r.Close()

		cnt := 0
		for r.Next() {
			_, ok := r.Record().(*Record)
			if !ok {
				t.Errorf("%s:failed assertion:%v", tt.Name, r.Record())
			}
			cnt++
		}
		if r.Error() != nil {
			if !strings.Contains(r.Error().Error(), tt.Error) {
				t.Errorf("%s:unexpected error:%v", tt.Name, r.Error())
			}
		}
		if cnt != tt.Cnt {
			t.Errorf(
				"%s:wrong count: expected %d, actual %d:", tt.Name, tt.Cnt,
				cnt)
		}

		if r.Next() {
			t.Errorf("%s:Next() should return false.", tt.Name)
		}
	}
}
