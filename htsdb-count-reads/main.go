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
var CountBuilder = squirrel.Select("rname", "strand").
	Column(squirrel.Alias(squirrel.Expr("COUNT(*)"), "count")).
	Column(squirrel.Alias(squirrel.Expr("SUM(copy_number)"), "copyNum"))

// Count is a databases row with record count information.
type Count struct {
	Chrom   string `db:"rname"`
	Ori     int    `db:"strand"`
	Count   int    `db:"count"`
	CopyNum int    `db:"copyNum"`
}

const prog = "htsdb-count-reads"
const version = "0.3"
const descr = `Print the number of reads and read copies stored in the
database. Supports grouping by reference, orientation or both. Provided SQL
filter will apply to all counts.`

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
	groupByChrom = app.Flag("by-ref", "Group counts by reference.").
			Bool()
	groupByOri = app.Flag("by-ori", "Group counts by orientation.").
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
	if *groupByChrom == true {
		countBuilder = countBuilder.GroupBy("rname")
	}
	if *groupByOri == true {
		countBuilder = countBuilder.GroupBy("strand")
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
	if *groupByChrom == true && *groupByOri == true {
		if *header == true {
			fmt.Printf("category\tref\tori\tcount\tcopyNumber\n")
		}
		for _, c := range counts {
			fmt.Printf("%s\t%s\t%d\t%d\t%d\n", *as, c.Chrom, c.Ori, c.Count, c.CopyNum)
		}
	} else if *groupByChrom == true {
		if *header == true {
			fmt.Printf("category\tref\tcount\tcopyNumber\n")
		}
		for _, c := range counts {
			fmt.Printf("%s\t%s\t%d\t%d\n", *as, c.Chrom, c.Count, c.CopyNum)
		}
	} else if *groupByOri == true {
		if *header == true {
			fmt.Printf("category\tori\tcount\tcopyNumber\n")
		}
		for _, c := range counts {
			fmt.Printf("%s\t%d\t%d\t%d\n", *as, c.Ori, c.Count, c.CopyNum)
		}
	} else {
		if *header == true {
			fmt.Printf("category\tcount\tcopyNumber\n")
		}
		for _, c := range counts {
			fmt.Printf("%s\t%d\t%d\n", *as, c.Count, c.CopyNum)
		}
	}
}
