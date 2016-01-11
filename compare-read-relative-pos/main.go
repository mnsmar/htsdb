package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/biogo/biogo/feat"
	"github.com/jmoiron/sqlx"
	"github.com/mnsmar/htsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

const prog = "compare-read-relative-pos"
const version = "0.1"
const descr = `Measure head/tail read positions around reference head/tail
positions. Output is a delimited file with the number of reads that end at
each position around the reference ends..`

var (
	app     = kingpin.New(prog, descr)
	dbFile1 = app.Flag("db1", "SQLite database file.").PlaceHolder("<file>").Required().String()
	tab1    = app.Flag("table1", "Database table with aligned reads.").Default("sample").String()
	where1  = app.Flag("where1", "SQL query to be part of the WHERE clause.").PlaceHolder("<SQL>").String()
	dbFile2 = app.Flag("db2", "SQLite database file.").PlaceHolder("<file>").Required().String()
	tab2    = app.Flag("table2", "Database table with aligned reads.").Default("sample").String()
	where2  = app.Flag("where2", "SQL query to be part of the WHERE clause.").PlaceHolder("<SQL>").String()
	from    = app.Flag("pos", "Read position to measure.").Required().PlaceHolder("<head|tail>").Enum("head", "tail")
	anti    = app.Flag("anti", "Consider anti-sense reads instead of sense.").Bool()
	span    = app.Flag("span", "Region to measure, around reference ends.").Default("100").PlaceHolder("<int>").Int()
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
	if *where1 != "" {
		readsBuilder1 = readsBuilder1.Where(*where1)
	}
	readsBuilder2 := htsdb.RangeBuilder.From(*tab2)
	refsBuilder2 := htsdb.ReferenceBuilder.From(*tab2)
	if *where2 != "" {
		readsBuilder2 = readsBuilder2.Where(*where2)
		refsBuilder2 = refsBuilder2.Where(*where2)
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
	refs, err := htsdb.SelectReferences(db2, refsBuilder2)

	// get position extracting function
	getPos := htsdb.Head
	if *from == "tail" {
		getPos = htsdb.Tail
	}

	// count histogram around reference.
	hists := make(chan map[int]uint)
	var wg sync.WaitGroup
	for _, ref := range refs {
		for _, ori := range []feat.Orientation{feat.Forward, feat.Reverse} {
			wg.Add(1)
			go func(ori feat.Orientation, ref htsdb.Reference) {
				if *verbose == true {
					log.Printf("orient:%s, chrom:%s\n", ori, ref.Chrom)
				}
				defer wg.Done()
				var r htsdb.Range

				wig := make(map[int]uint)
				ori1 := ori
				if *anti == true {
					ori1 = -1 * ori1
				}
				rows1, err := readsStmt1.Queryx(ori1, ref.Chrom)
				panicOnError(err)
				for rows1.Next() {
					err = rows1.StructScan(&r)
					panicOnError(err)
					pos := getPos(&r, ori1)
					wig[pos]++
				}

				hist := make(map[int]uint)
				rows2, err := readsStmt2.Queryx(ori, ref.Chrom)
				panicOnError(err)
				for rows2.Next() {
					err = rows2.StructScan(&r)
					panicOnError(err)
					pos := getPos(&r, ori)
					for relPos := -*span; relPos <= *span; relPos++ {
						if pos+relPos < 0 {
							continue
						}
						hist[relPos*int(ori)] += wig[pos+relPos]
					}
				}
				hists <- hist
			}(ori, ref)
		}
	}

	go func() {
		wg.Wait()
		close(hists)

	}()

	// aggregate histograms from goroutines
	aggrHist := make(map[int]uint)
	for hist := range hists {
		for k, v := range hist {
			aggrHist[k] += v
		}
	}

	// print results.
	fmt.Printf("pos\tcount\n")
	for i := -*span; i <= *span; i++ {
		fmt.Printf("%d\t%d\n", i, aggrHist[i])
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
