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

const prog = "htsdb-relative-pos-dist"
const version = "0.2"
const descr = `Measure relative position distribution for reads in database 1
against reads in database 2. For each possible relative position, print the
number of read pairs with this relative positioning, the total number of
possible pairs and the total number of reads in each database. Read relative
position is measured either 5'-5' or 3'-3'. Positive numbers indicate read 1
is downstream of read 2. Provided SQL
filters will apply to all counts.`

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

func main() {
	app.HelpFlag.Short('h')
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("%s", err)
	}

	// assemble sqlx select builders
	readsBuilder1 := htsdb.RangeBuilder.From(*tab1)
	countBuilder1 := htsdb.CountBuilder.From(*tab1)
	if *where1 != "" {
		readsBuilder1 = readsBuilder1.Where(*where1)
		countBuilder1 = countBuilder1.Where(*where1)
	}
	readsBuilder2 := htsdb.RangeBuilder.From(*tab2)
	refsBuilder2 := htsdb.ReferenceBuilder.From(*tab2)
	countBuilder2 := htsdb.CountBuilder.From(*tab2)
	if *where2 != "" {
		readsBuilder2 = readsBuilder2.Where(*where2)
		refsBuilder2 = refsBuilder2.Where(*where2)
		countBuilder2 = countBuilder2.Where(*where2)
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

	// count records
	countQuery1, _, err := countBuilder1.ToSql()
	panicOnError(err)
	var totalCount1 int
	if err = db1.Get(&totalCount1, countQuery1); err != nil {
		panic(err)
	}
	countQuery2, _, err := countBuilder2.ToSql()
	panicOnError(err)
	var totalCount2 int
	if err = db2.Get(&totalCount2, countQuery2); err != nil {
		panic(err)
	}

	// get position extracting function
	getPos := htsdb.Head
	if *from == "3p" {
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
	fmt.Printf("pos\tpairs\treadCount1\treadCount2\n")
	for i := -*span; i <= *span; i++ {
		fmt.Printf("%d\t%d\t%d\t%d\n", i, aggrHist[i], totalCount1, totalCount2)
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
