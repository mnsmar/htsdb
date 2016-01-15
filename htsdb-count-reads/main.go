package main

import (
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Count struct {
	Chrom   string `db:"rname"`
	Length  int    `db:"length"`
	Strand  int    `db:"strand"`
	Count   int    `db:"count"`
	CopyNum int    `db:"copyNum"`
}

// CountBuilder is a squirrel select builder whose columns match Count fields.
var CountBuilder = squirrel.Select("rname", "strand").
	Column(squirrel.Alias(squirrel.Expr("COUNT(*)"), "count")).
	Column(squirrel.Alias(squirrel.Expr("SUM(copy_number)"), "copyNum")).
	GroupBy("rname", "strand")

const prog = "htsdb-count-reads"
const version = "0.2"
const descr = `Print the number of reads and read copies stored in the
database grouped by reference and orientation. Provided SQL filter will apply
to all counts.`

var (
	app = kingpin.New(prog, descr)

	dbFile = app.Flag("db", "SQLite file for database.").
		PlaceHolder("<file>").Required().String()
	tab = app.Flag("table", "Database table name for db.").
		Default("sample").String()
	where = app.Flag("where", "SQL filter injected in WHERE clause for db.").
		PlaceHolder("<SQL>").String()
	sum = app.Flag("total", "Print a grand total.").
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
	var total, totalCN int
	fmt.Printf("ref\tori\tcount\tcopyNumber\n")
	for _, c := range counts {
		total += c.Count
		totalCN += c.CopyNum
		fmt.Printf("%s\t%d\t%d\t%d\n", c.Chrom, c.Strand, c.Count, c.CopyNum)
	}
	if *sum == true {
		fmt.Printf("total\tNA\t%d\t%d\n", total, totalCN)
	}
}
