package main

import (
	"fmt"
	"os"

	"github.com/Masterminds/squirrel"
	_ "github.com/mattn/go-sqlite3"

	"github.com/jmoiron/sqlx"
	"gopkg.in/alecthomas/kingpin.v2"
)

// CountBuilder is a squirrel select builder whose columns match Count fields.
var CountBuilder = squirrel.Select().
	Column(squirrel.Alias(squirrel.Expr("COUNT(*)"), "total")).
	Column(squirrel.Alias(squirrel.Expr("SUM(copy_number)"), "copyNum"))

// Reference is a reference feature on which reads align.
type Count struct {
	Total   int `db:"total"`
	CopyNum int `db:"copyNum"`
}

const prog = "htsdb-count-reads"
const version = "0.1"
const descr = `Count number of reads. Output is a delimited file with the
number of reads and the corresponding total copy number.`

var (
	app    = kingpin.New(prog, descr)
	dbFile = app.Flag("db", "SQLite database file.").PlaceHolder("<file>").Required().String()
	tab    = app.Flag("table", "Name of table with aligned reads.").Default("sample").String()
	where  = app.Flag("where", "SQL query to be part of the WHERE clause.").PlaceHolder("<SQL>").String()
	as     = app.Flag("as", "Name for output count.").Default("all").String()
	header = app.Flag("header", "Write header line to output.").Bool()
)

func main() {
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
	var count Count
	if err = db.Get(&count, query); err != nil {
		panic(err)
	}

	// print results.
	if *header == true {
		fmt.Printf("category\tcount\tcopy_number\n")
	}
	fmt.Printf("%s\t%d\t%d\n", *as, count.Total, count.CopyNum)
}
