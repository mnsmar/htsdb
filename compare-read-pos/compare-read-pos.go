package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/jmoiron/sqlx"
	"github.com/mnsmar/htsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

const prog = "compare-read-positions"
const version = "0.1"
const descr = `Measure the head/tail read positions that are occupied by
reference head/tail positions and the reads on these positions. A head/tail
position is occupied when an equal reference head/tail position exists.`

type count struct {
	posTotal, posOccupied, readsTotal, readsOccupied int
}

func (c *count) incrementBy(inc *count) {
	c.posTotal += inc.posTotal
	c.posOccupied += inc.posOccupied
	c.readsTotal += inc.readsTotal
	c.readsOccupied += inc.readsOccupied
}

func (c *count) percentPosOccupied() float64 {
	return (float64(c.posOccupied) / float64(c.posTotal)) * 100
}

func (c *count) percentReadsOccupied() float64 {
	return (float64(c.readsOccupied) / float64(c.readsTotal)) * 100
}

var (
	app     = kingpin.New(prog, descr)
	dbFile1 = app.Flag("db1", "SQLite database file.").PlaceHolder("<file>").Required().String()
	tab1    = app.Flag("table1", "Database table with aligned reads.").Default("sample").String()
	where1  = app.Flag("where1", "SQL query to be part of the WHERE clause.").PlaceHolder("<SQL>").String()
	dbFile2 = app.Flag("db2", "SQLite database file.").PlaceHolder("<file>").Required().String()
	tab2    = app.Flag("table2", "Database table with aligned reads.").Default("sample").String()
	where2  = app.Flag("where2", "SQL query to be part of the WHERE clause.").PlaceHolder("<SQL>").String()
	from    = app.Flag("pos", "Read position on which to measure occupancy.").Required().PlaceHolder("<head|tail>").Enum("head", "tail")
	verbose = app.Flag("verbose", "Verbose mode.").Short('v').Bool()
)

func main() {
	app.HelpFlag.Short('h')
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("%s", err)
	}

	// assemble sqlx select builders
	readsBuilder1 := htsdb.RangeBuilder.From(*tab1)
	refsBuilder1 := htsdb.ReferenceBuilder.From(*tab1)
	if *where1 != "" {
		readsBuilder1 = readsBuilder1.Where(*where1)
		refsBuilder1 = refsBuilder1.Where(*where1)
	}
	readsBuilder2 := htsdb.RangeBuilder.From(*tab2)
	if *where2 != "" {
		readsBuilder2 = readsBuilder2.Where(*where2)
	}

	// open database connections.
	var db1, db2 *sqlx.DB
	if db1, err = sqlx.Connect("sqlite3", *dbFile1); err != nil {
		panic(err)
	}
	if db2, err = sqlx.Connect("sqlite3", *dbFile2); err != nil {
		panic(err)
	}

	// prepare statements.
	query1, _, err := readsBuilder1.Where("strand = ? AND rname = ?").ToSql()
	panicOnError(err)
	readsStmt1, err := db1.Preparex(query1)
	panicOnError(err)
	query2, _, err := readsBuilder2.Where("strand = ? AND rname = ?").ToSql()
	panicOnError(err)
	readsStmt2, err := db2.Preparex(query2)
	panicOnError(err)

	// select reference features
	refs, err := htsdb.SelectReferences(db1, refsBuilder1)

	// get position extracting function
	getPos := head
	if *from == "tail" {
		getPos = tail
	}

	// count occupied positions.
	counts := make(chan (*count))
	var wg sync.WaitGroup
	for _, ref := range refs {
		for _, strand := range []int{-1, 1} {
			wg.Add(1)
			go func(strand int, chrom string) {
				defer wg.Done()
				cnt := &count{}
				var r htsdb.Range
				if *verbose == true {
					log.Printf("strand:%d, chromosome:%s\n", strand, chrom)
				}

				occupied := make(map[int]bool)

				rows2, err := readsStmt2.Queryx(strand, chrom)
				panicOnError(err)
				for rows2.Next() {
					err = rows2.StructScan(&r)
					panicOnError(err)
					pos := getPos(&r, strand)
					occupied[pos] = true
				}

				rows1, err := readsStmt1.Queryx(strand, chrom)
				panicOnError(err)
				for rows1.Next() {
					err = rows1.StructScan(&r)
					panicOnError(err)
					pos := getPos(&r, strand)
					if occupied[pos] {
						cnt.posOccupied++
						cnt.readsOccupied += r.CopyNumber
					}
					cnt.posTotal++
					cnt.readsTotal += r.CopyNumber
				}
				counts <- cnt
			}(strand, ref.Chrom)
		}
	}

	go func() {
		wg.Wait()
		close(counts)

	}()

	// aggregate counts from goroutines
	aggr := &count{}
	for v := range counts {
		aggr.incrementBy(v)
	}

	// print results.
	fmt.Printf("total_pos:%d\noccupied_pos:%d\npercent_pos:%.2f\n"+
		"total_reads:%d\noccupied_reads:%d\npercent_reads:%.2f\n",
		aggr.posTotal, aggr.posOccupied, aggr.percentPosOccupied(),
		aggr.readsTotal, aggr.readsOccupied, aggr.percentReadsOccupied())
}

func head(r *htsdb.Range, strand int) int {
	if strand == 1 {
		return r.StartPos
	} else if strand == -1 {
		return r.StopPos
	}
	panic("hist-around-reference: strand is not 1 or -1")
}

func tail(r *htsdb.Range, strand int) int {
	if strand == 1 {
		return r.StopPos
	} else if strand == -1 {
		return r.StartPos
	}
	panic("hist-around-reference: strand is not 1 or -1")
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
