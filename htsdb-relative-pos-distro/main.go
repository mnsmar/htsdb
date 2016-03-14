package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Masterminds/squirrel"
	_ "github.com/mattn/go-sqlite3"

	"github.com/biogo/biogo/feat"
	"github.com/jmoiron/sqlx"
	"github.com/mnsmar/htsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

const prog = "htsdb-relative-pos-distro"
const version = "0.3"
const descr = `Measure the distribution of the relative position of reads in
database 1 against reads in database 2. For each relative position in the
provided span, print the number of read pairs with this relative positioning
in the two databases, the total number of possible pairs and the total number
of reads in each database. Read relative position is measured either 5'-5' or
3'-3'. Positive numbers indicate read 1 is downstream of read 2. Provided SQL
filters will apply to all counts.`
const maxConc = 512

var (
	app = kingpin.New(prog, descr)

	dbFile1 = app.Flag("db1", "SQLite file for database 1.").
		PlaceHolder("<file>").Required().String()
	tab1 = app.Flag("table1", "Database table name for db1.").
		Default("sample").String()
	where1 = app.Flag("where1", "SQL filter injected in WHERE clause for db1.").
		PlaceHolder("<SQL>").String()
	dbFile2 = app.Flag("db2", "SQLite file for database 2.").
		PlaceHolder("<file>").Required().String()
	tab2 = app.Flag("table2", "Database table name for db2.").
		Default("sample").String()
	where2 = app.Flag("where2", "SQL filter injected in WHERE clause for db2.").
		PlaceHolder("<SQL>").String()
	from = app.Flag("pos", "Reference point for relative position measurement.").
		Required().PlaceHolder("<5p|3p>").Enum("5p", "3p")
	anti = app.Flag("anti", "Compare reads on opposite instead of same orientation.").
		Bool()
	span = app.Flag("span", "Maximum distance between compared reads.").
		Default("100").PlaceHolder("<int>").Int()
	verbose = app.Flag("verbose", "Verbose mode.").Short('v').Bool()
)

type job struct {
	verbose    bool
	ref        htsdb.Reference
	ori        feat.Orientation
	readsStmt1 *sqlx.Stmt
	readsStmt2 *sqlx.Stmt
	span       int
	getPos     func(feat.Range, feat.Orientation) int
}

type result struct {
	hist map[int]uint
	job  job
}

func worker(id int, jobs <-chan job, results chan<- result) {
	for j := range jobs {
		if j.verbose == true {
			log.Printf("wID:%d, orient:%s, chrom:%s\n", id, j.ori, j.ref.Chrom)
		}
		var r htsdb.Range

		wig := make(map[int]uint)
		ori1 := j.ori
		if *anti == true {
			ori1 = -1 * ori1
		}
		rows1, err := j.readsStmt1.Queryx(ori1, j.ref.Chrom)
		if err != nil {
			log.Fatal(err)
		}
		for rows1.Next() {
			err = rows1.StructScan(&r)
			if err != nil {
				log.Fatal(err)
			}
			pos := j.getPos(&r, ori1)
			wig[pos]++
		}

		hist := make(map[int]uint)
		rows2, err := j.readsStmt2.Queryx(j.ori, j.ref.Chrom)
		if err != nil {
			log.Fatal(err)
		}
		for rows2.Next() {
			err = rows2.StructScan(&r)
			if err != nil {
				log.Fatal(err)
			}
			pos := j.getPos(&r, j.ori)
			for relPos := -j.span; relPos <= j.span; relPos++ {
				if pos+relPos < 0 {
					continue
				}
				hist[relPos*int(j.ori)] += wig[pos+relPos]
			}
		}
		results <- result{hist: hist, job: j}
	}
}

func main() {
	app.HelpFlag.Short('h')
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("%s", err)
	}

	// open database connections.
	db1 := connectDB(*dbFile1)
	db2 := connectDB(*dbFile2)

	// assemble sqlx select builders
	readsB1, _, countB1 := newBuilders(*tab1, *where1)
	readsB2, refsB2, countB2 := newBuilders(*tab2, *where2)

	// prepare statements.
	readsStmt1, err := prepareStmt(readsB1.Where("strand = ? AND rname = ?"), db1)
	if err != nil {
		log.Fatal(err)
	}
	readsStmt2, err := prepareStmt(readsB2.Where("strand = ? AND rname = ?"), db2)
	if err != nil {
		log.Fatal(err)
	}

	// select reference features
	refs, err := htsdb.SelectReferences(db2, refsB2)

	// count records
	var totalCount1, totalCount2 int
	if totalCount1, err = countRecords(countB1, db1); err != nil {
		log.Fatal(err)
	}
	if totalCount2, err = countRecords(countB2, db2); err != nil {
		log.Fatal(err)
	}

	// get position extracting function
	getPos := htsdb.Head
	if *from == "3p" {
		getPos = htsdb.Tail
	}

	jobs := make(chan job, 100000)
	results := make(chan result, 100)

	for w := 1; w <= maxConc; w++ {
		go worker(w, jobs, results)
	}

	jobCnt := 0
	for _, ref := range refs {
		for _, ori := range []feat.Orientation{feat.Forward, feat.Reverse} {
			jobCnt++
			jobs <- job{
				verbose:    *verbose,
				ref:        ref,
				ori:        ori,
				readsStmt1: readsStmt1,
				readsStmt2: readsStmt2,
				span:       *span,
				getPos:     getPos,
			}
		}
	}
	close(jobs)

	// aggregate histograms from goroutines
	aggrHist := make(map[int]uint)
	for a := 0; a < jobCnt; a++ {
		res := <-results
		hist := res.hist
		for k, v := range hist {
			aggrHist[k] += v
		}
	}

	// print results.
	fmt.Printf("pos\tpairs\treadCount1\treadCount2\n")
	for i := -*span; i <= *span; i++ {
		fmt.Printf("%d\t%d\t%d\t%d\n", i, aggrHist[i], totalCount1, totalCount2)
	}
}

func newBuilders(tab, where string) (
	readsB, refsB, countB squirrel.SelectBuilder) {

	readsB = htsdb.RangeBuilder.From(tab)
	refsB = htsdb.ReferenceBuilder.From(tab)
	countB = htsdb.CountBuilder.From(tab)
	if where != "" {
		readsB = readsB.Where(where)
		refsB = refsB.Where(where)
		countB = countB.Where(where)
	}
	return readsB, refsB, countB
}

func prepareStmt(b squirrel.SelectBuilder, db *sqlx.DB) (*sqlx.Stmt, error) {
	q, _, err := b.ToSql()
	if err != nil {
		return nil, err
	}
	stmt, err := db.Preparex(q)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func countRecords(b squirrel.SelectBuilder, db *sqlx.DB) (int, error) {
	var count int

	q, _, err := b.ToSql()
	if err != nil {
		return count, err
	}
	err = db.Get(&count, q)

	return count, err
}

func connectDB(file string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", file)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
