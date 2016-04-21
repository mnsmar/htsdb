package main

import (
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Masterminds/squirrel"
	"github.com/biogo/biogo/feat"
	"github.com/jmoiron/sqlx"
	"github.com/mnsmar/htsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

const prog = "htsdb-relative-pos-distro"
const version = "0.4"
const descr = `Measure the distribution of the relative position of reads in
database 1 against reads in database 2. For each relative position in the
provided span, print the number of read pairs with this relative positioning
in the two databases, the total number of possible pairs and the total number
of reads in each database. Read relative position is measured either 5'-5' or
3'-3'. Positive numbers indicate read 1 is downstream of read 2. Supports
grouping by reference. Provided SQL filters will apply to all counts.`
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
	groupByChrom = app.Flag("by-ref", "Group counts by reference.").
			Bool()
	verbose = app.Flag("verbose", "Verbose mode.").Short('v').Bool()
)

type job struct {
	verbose      bool
	ref          htsdb.Reference
	decs1, decs2 []BuilderDecorator
	db1, db2     *sqlx.DB
	span         int
	getPos       func(feat.Range, feat.Orientation) int
}

type result struct {
	hist   map[int]uint
	count1 int
	count2 int
	job    job
}

func worker(id int, jobs <-chan job, results chan<- result) {
	for j := range jobs {
		if j.verbose == true {
			log.Printf("wID:%d, chrom:%s\n", id, j.ref.Chrom)
		}

		var err error
		var r htsdb.Range

		// assemble sqlx select builders
		rangeDec := Where("strand = ? AND rname = ?")
		readsB1 := DecorateBuilder(htsdb.RangeBuilder, append(j.decs1, rangeDec)...)
		readsB2 := DecorateBuilder(htsdb.RangeBuilder, append(j.decs2, rangeDec)...)

		// prepare statements.
		var readsStmt1, readsStmt2 *sqlx.Stmt
		if readsStmt1, err = prepareStmt(readsB1, j.db1); err != nil {
			log.Fatal(err)
		}
		if readsStmt2, err = prepareStmt(readsB2, j.db2); err != nil {
			log.Fatal(err)
		}

		hist := make(map[int]uint)
		var count1, count2 int
		for _, ori := range []feat.Orientation{feat.Forward, feat.Reverse} {
			// loop on reads in db1.
			wig := make(map[int]uint)
			ori1 := ori
			if *anti == true {
				ori1 = -1 * ori1
			}
			rows1, err := readsStmt1.Queryx(ori1, j.ref.Chrom)
			if err != nil {
				log.Fatal(err)
			}
			for rows1.Next() {
				if err = rows1.StructScan(&r); err != nil {
					log.Fatal(err)
				}
				count1++
				pos := j.getPos(&r, ori1)
				wig[pos]++
			}

			// loop on reads in db2.
			rows2, err := readsStmt2.Queryx(ori, j.ref.Chrom)
			if err != nil {
				log.Fatal(err)
			}
			for rows2.Next() {
				if err = rows2.StructScan(&r); err != nil {
					log.Fatal(err)
				}
				count2++
				pos := j.getPos(&r, ori)
				for relPos := -j.span; relPos <= j.span; relPos++ {
					if pos+relPos < 0 {
						continue
					}
					hist[relPos*int(ori)] += wig[pos+relPos]
				}
			}
		}

		// enqueue in results channel
		results <- result{hist: hist, job: j, count1: count1, count2: count2}
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

	// the decorators that apply required SQL clauses on each connection.
	decs1 := []BuilderDecorator{Table(*tab1), Where(*where1)}
	decs2 := []BuilderDecorator{Table(*tab2), Where(*where2)}

	// select reference features
	refsB1 := DecorateBuilder(htsdb.ReferenceBuilder, decs1...)
	refs1, err := htsdb.SelectReferences(db1, refsB1)
	if err != nil {
		log.Fatal(err)
	}
	refsB2 := DecorateBuilder(htsdb.ReferenceBuilder, decs2...)
	refs2, err := htsdb.SelectReferences(db2, refsB2)
	if err != nil {
		log.Fatal(err)
	}
	refs := make(map[string]htsdb.Reference)
	for _, r := range append(refs1, refs2...) {
		refs[r.Chrom] = r
	}

	// get position extracting function
	getPos := htsdb.Head
	if *from == "3p" {
		getPos = htsdb.Tail
	}

	// deploy workers
	jobs := make(chan job, 100000)
	results := make(chan result, 100)
	for w := 1; w <= maxConc; w++ {
		go worker(w, jobs, results)
	}

	// create the jobs
	jobCnt := 0
	for _, ref := range refs {
		jobCnt++
		jobs <- job{
			verbose: *verbose,
			ref:     ref,
			db1:     db1,
			db2:     db2,
			decs1:   decs1,
			decs2:   decs2,
			span:    *span,
			getPos:  getPos,
		}
	}
	close(jobs)

	// collect results for all jobs in a slice
	var allResults []result
	for a := 0; a < jobCnt; a++ {
		allResults = append(allResults, <-results)
	}

	// print output
	if *groupByChrom == true {
		fmt.Printf("ref\tpos\tpairs\treadCount1\treadCount2\n")
		for _, res := range allResults {
			for i := -*span; i <= *span; i++ {
				fmt.Printf("%s\t%d\t%d\t%d\t%d\n",
					res.job.ref.Name(), i, res.hist[i], res.count1, res.count2)
			}
		}
	} else {
		var totalCount1, totalCount2 int
		aggrHist := make(map[int]uint)
		for _, res := range allResults {
			for k, v := range res.hist {
				aggrHist[k] += v
			}
			totalCount1 += res.count1
			totalCount2 += res.count2
		}

		fmt.Printf("pos\tpairs\treadCount1\treadCount2\n")
		for i := -*span; i <= *span; i++ {
			fmt.Printf("%d\t%d\t%d\t%d\n", i, aggrHist[i], totalCount1, totalCount2)
		}
	}
}

// A BuilderDecorator wraps a squirrel.SelectBuilder with extra behaviour.
type BuilderDecorator func(squirrel.SelectBuilder) squirrel.SelectBuilder

// Table returns a BuilderDecorator that extends a squirrel.SelectBuilder with
// the table property.
func Table(table string) BuilderDecorator {
	return func(b squirrel.SelectBuilder) squirrel.SelectBuilder {
		return b.From(table)
	}
}

// Where returns a BuilderDecorator that extends a squirrel.SelectBuilder with
// a where clause. Returns the builder itself if the where clause is the empty
// string.
func Where(clause string) BuilderDecorator {
	return func(b squirrel.SelectBuilder) squirrel.SelectBuilder {
		if clause != "" {
			return b.Where(clause)
		}
		return b
	}
}

//DecorateBuilder decorates a squirrel.SelectBuilder with all the given
//BuilderDecorators, in order.
func DecorateBuilder(b squirrel.SelectBuilder, ds ...BuilderDecorator) squirrel.SelectBuilder {
	decorated := b
	for _, decorate := range ds {
		decorated = decorate(decorated)
	}
	return decorated
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

func connectDB(file string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", file)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
