package main

import (
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"gopkg.in/alecthomas/kingpin.v2"
)

// CountBuilder is a squirrel select builder whose columns match Count fields.
var CountBuilder = squirrel.Select().
	Column(squirrel.Alias(squirrel.Expr("LENGTH(sequence)"), "len")).
	Column(squirrel.Alias(squirrel.Expr("COUNT(*)"), "count")).
	Column(squirrel.Alias(squirrel.Expr("SUM(copy_number)"), "copyNum")).
	GroupBy("len").OrderBy("len")

// Count is a databases row with record count information.
type Count struct {
	SeqLen  int `db:"len"`
	Count   int `db:"count"`
	CopyNum int `db:"copyNum"`
}

const prog = "htsdb-size-distro"
const version = "0.1"
const descr = `Print the number of reads and read copies for each read size.
Provided SQL filter will apply to all counts.`

var (
	app = kingpin.New(prog, descr)

	dbFile = app.Flag("db", "File to SQLite database.").
		PlaceHolder("<file>").Required().String()
	tab = app.Flag("table", "Database table name.").
		Default("sample").String()
	where = app.Flag("where", "SQL filter to inject in WHERE clause.").
		PlaceHolder("<SQL>").String()
	as = app.Flag("as", "Name to print describing the count/s.").
		Default("all").String()
	header = app.Flag("header", "Print header line.").
		Bool()
)

func main() {
	// read command line args and options
	app.HelpFlag.Short('h')
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("%s", err)
	}

	// assemble sqlx select builders
	countBuilder := CountBuilder.From(*tab)
	if *where != "" {
		countBuilder = countBuilder.Where(*where)
	}

	// open database connections.
	var db *sqlx.DB
	if db, err = sqlx.Connect("sqlite3", *dbFile); err != nil {
		panic(err)
	}

	// prepare statements.
	query, _, err := countBuilder.ToSql()
	if err != nil {
		panic(err)
	}

	// get the count
	var counts []Count
	if err = db.Select(&counts, query); err != nil {
		panic(err)
	}

	// print results.
	if *header == true {
		fmt.Printf("category\tlen\tcount\tcopyNumber\n")
	}
	idx := 0
	for _, c := range counts {
		for c.SeqLen > idx {
			fmt.Printf("%s\t%d\t%d\t%d\n", *as, idx, 0, 0)
			idx++
		}
		idx = c.SeqLen + 1
		fmt.Printf("%s\t%d\t%d\t%d\n", *as, c.SeqLen, c.Count, c.CopyNum)
	}
}
