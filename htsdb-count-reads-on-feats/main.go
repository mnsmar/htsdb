package main

import (
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Masterminds/squirrel"
	"github.com/biogo/biogo/io/featio"
	"github.com/biogo/biogo/io/featio/bed"
	"github.com/jmoiron/sqlx"
	"gopkg.in/alecthomas/kingpin.v2"
)

// CountBuilder is a squirrel select builder whose columns match Count fields.
var CountBuilder = squirrel.Select().
	Column(squirrel.Alias(squirrel.Expr("COUNT(*)"), "count")).
	Column(squirrel.Alias(squirrel.Expr("SUM(copy_number)"), "copyNum"))

// Count is a databases row with record count information.
type Count struct {
	Count   int `db:"count"`
	CopyNum int `db:"copyNum"`
}

const prog = "htsdb-count-reads-on-feats"
const version = "0.1"
const descr = `Print number of reads and read copies that are contained in
each feature of the input file. Currently only features in the BED6 format are
supported. Provided SQL filter will apply to all counts.`

var (
	app = kingpin.New(prog, descr)

	dbFile = app.Flag("db", "File to SQLite database.").
		PlaceHolder("<file>").Required().String()
	tab = app.Flag("table", "Database table name.").
		Default("sample").String()
	where = app.Flag("where", "SQL filter to inject in WHERE clause.").
		PlaceHolder("<SQL>").String()
	bed6 = app.Flag("bed6", "BED6 file with features.").
		PlaceHolder("<file>").Required().String()
	as = app.Flag("as", "Name to print describing the count/s.").
		Default("all").String()
	header = app.Flag("header", "Print header line.").
		Bool()
	useOri = app.Flag("use-ori", "Only report counts on the orientation of the feature.").
		Bool()
)

func main() {
	var err error
	var query string
	var db *sqlx.DB
	var stmt *sqlx.Stmt

	// read command line args and options
	app.HelpFlag.Short('h')
	app.Version(version)
	if _, err := app.Parse(os.Args[1:]); err != nil {
		kingpin.Fatalf("%s", err)
	}

	// assemble sqlx select builders
	countBuilder := CountBuilder.From(*tab).Where("rname = ? AND start BETWEEN ? AND ? AND stop BETWEEN ? AND ?")
	if *where != "" {
		countBuilder = countBuilder.Where(*where)
	}
	if *useOri == true {
		countBuilder = countBuilder.Where("strand = ?")
	}

	// open database connections.
	if db, err = sqlx.Connect("sqlite3", *dbFile); err != nil {
		panic(err)
	}

	// prepare statements.
	if query, _, err = countBuilder.ToSql(); err != nil {
		panic(err)
	}
	if stmt, err = db.Preparex(query); err != nil {
		panic(err)
	}

	// open BED6 scanner
	bedS, err := bed6Scanner(*bed6)
	if err != nil {
		panic(err)
	}

	// loop on the BED6 feats and count
	if *header == true {
		fmt.Printf("category\tfeat\tcount\tcopyNumber\n")
	}
	for {
		if bedS.Next() == false {
			break
		}
		b, _ := bedS.Feat().(*bed.Bed6)
		start, stop, ori, chrom := b.Start(), b.End()-1, b.Orientation(), b.Location().Name()

		var c Count
		if *useOri == true {
			stmt.Get(&c, chrom, start, stop, start, stop, ori)
		} else {
			stmt.Get(&c, chrom, start, stop, start, stop)
		}
		fmt.Printf("%s\t%s:%d-%d:%d\t%d\t%d\n", *as, chrom, start, stop, ori, c.Count, c.CopyNum)
	}
	if err = bedS.Error(); err != nil {
		panic(err)
	}
}

func bed6Scanner(f string) (*featio.Scanner, error) {
	ioR, err := os.Open(*bed6)
	if err != nil {
		return nil, err
	}
	bedR, err := bed.NewReader(ioR, 6)
	if err != nil {
		return nil, err
	}
	return featio.NewScanner(bedR), nil
}
