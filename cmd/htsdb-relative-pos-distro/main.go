package main

import (
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Masterminds/squirrel"
	"github.com/alexflint/go-arg"
	"github.com/biogo/biogo/feat"
	"github.com/jmoiron/sqlx"
	"github.com/mnsmar/htsdb"
)

const maxConc = 12

// Opts is the struct with the options that the program accepts.
type Opts struct {
	DB1       string `arg:"required,help:SQLite3 database 1"`
	Table1    string `arg:"required,help:table name for db1"`
	Where1    string `arg:"help:SQL filter injected in WHERE clause of db1"`
	Pos1      string `arg:"required,help:reference point for reads of db1; one of 5p or 3p"`
	Collapse1 bool   `arg:"help:Collapse reads that have the same pos1"`
	DB2       string `arg:"required,help:SQLite3 database 2"`
	Table2    string `arg:"required,help:table name for db2"`
	Where2    string `arg:"help:SQL filter injected in WHERE clause of db2"`
	Pos2      string `arg:"required,help:reference point for reads of db2; one of 5p or 3p"`
	Collapse2 bool   `arg:"help:collapse reads that have the same pos2"`
	Span      int    `arg:"required,help:maximum distance of compared pos"`
	GroupRef  bool   `arg:"--by-ref,help:group counts by reference"`
	Anti      bool   `arg:"help:Compare reads on opposite instead of same orientation"`
	Verbose   bool   `arg:"-v,help:report progress"`
}

// Version returns the program version.
func (Opts) Version() string {
	return "htsdb-relative-pos-distro 0.6"
}

// Description returns an extended description of the program.
func (Opts) Description() string {
	return "Measure distribution of read relative positions in database 1 against database 2. Prints the number of read pairs at each relative position along with the total number of possible pairs and the total number of reads in each database. Positive relative positions indicate read 1 is downstream of read 2. Provided SQL filters will apply to all counts."
}

func main() {
	var err error
	var opts Opts
	var db1, db2 *sqlx.DB

	p := arg.MustParse(&opts)
	if opts.Pos1 != "5p" && opts.Pos1 != "3p" {
		p.Fail("--pos1 must be either 5p or 3p")
	}
	if opts.Pos2 != "5p" && opts.Pos2 != "3p" {
		p.Fail("--pos2 must be either 5p or 3p")
	}

	// open database connections.
	if db1, err = sqlx.Connect("sqlite3", opts.DB1); err != nil {
		log.Fatal(err)
	}
	if db2, err = sqlx.Connect("sqlite3", opts.DB2); err != nil {
		log.Fatal(err)
	}

	// create select decorators.
	decors1 := []BuilderDecorator{Table(opts.Table1), Where(opts.Where1)}
	decors2 := []BuilderDecorator{Table(opts.Table2), Where(opts.Where2)}

	// extract reference features
	refs, err := readRefs(db1, db2, decors1, decors2)
	if err != nil {
		log.Fatal("error reading BED:", err)
	}

	// goroutine that sends each reference as a job to jobs.
	jobs := make(chan job)
	go func() {
		for _, ref := range refs {
			jobs <- job{
				opts:    opts,
				ref:     ref,
				db1:     db1,
				db2:     db2,
				decors1: decors1,
				decors2: decors2,
			}
		}
		close(jobs)
	}()

	// start workers that consume jobs and send results to results.
	results := make(chan result)
	var wg sync.WaitGroup
	wg.Add(maxConc)
	for w := 1; w <= maxConc; w++ {
		go func() {
			worker(w, jobs, results)
			wg.Done()
		}()
	}

	// goroutine that checks when all workers are done and closes results.
	go func() {
		wg.Wait()
		close(results)
	}()

	// print output
	if opts.GroupRef == true {
		fmt.Printf("ref\tpos\tpairs\treadCount1\treadCount2\n")
		for res := range results {
			for i := -opts.Span; i <= opts.Span; i++ {
				fmt.Printf("%s\t%d\t%d\t%d\t%d\n",
					res.job.ref.Name(), i, res.hist[i], res.count1, res.count2)
			}
		}
	} else {
		var totalCount1, totalCount2 int
		aggrHist := make(map[int]uint)
		for res := range results {
			for k, v := range res.hist {
				aggrHist[k] += v
			}
			totalCount1 += res.count1
			totalCount2 += res.count2
		}

		fmt.Printf("pos\tpairs\treadCount1\treadCount2\n")
		for i := -opts.Span; i <= opts.Span; i++ {
			fmt.Printf("%d\t%d\t%d\t%d\n", i, aggrHist[i], totalCount1, totalCount2)
		}
	}
}

func worker(id int, jobs <-chan job, results chan<- result) {
	for j := range jobs {
		if j.opts.Verbose == true {
			log.Printf("wID:%d, chrom:%s\n", id, j.ref.Name())
		}

		var err error
		var r htsdb.Range

		// assemble sqlx select builders
		rangeDec := Where("strand = ? AND rname = ?")
		readsB1 := DecorateBuilder(htsdb.RangeBuilder, append(j.decors1, rangeDec)...)
		readsB2 := DecorateBuilder(htsdb.RangeBuilder, append(j.decors2, rangeDec)...)

		// prepare statements.
		var readsStmt1, readsStmt2 *sqlx.Stmt
		if readsStmt1, err = prepareStmt(readsB1, j.db1); err != nil {
			log.Fatal(err)
		}
		if readsStmt2, err = prepareStmt(readsB2, j.db2); err != nil {
			log.Fatal(err)
		}
		// get position extracting function
		getPos1 := htsdb.Head
		if j.opts.Pos1 == "3p" {
			getPos1 = htsdb.Tail
		}
		getPos2 := htsdb.Head
		if j.opts.Pos2 == "3p" {
			getPos2 = htsdb.Tail
		}

		hist := make(map[int]uint)
		var count1, count2 int
		for _, ori := range []feat.Orientation{feat.Forward, feat.Reverse} {
			// loop on reads in db1.
			wig := make(map[int]uint)
			ori1 := ori
			if j.opts.Anti == true {
				ori1 = -1 * ori1
			}
			rows1, err := readsStmt1.Queryx(ori1, j.ref.Name())
			if err != nil {
				log.Fatal(err)
			}
			for rows1.Next() {
				if err = rows1.StructScan(&r); err != nil {
					log.Fatal(err)
				}
				pos := getPos1(&r, ori1)
				if _, ok := wig[pos]; ok && j.opts.Collapse1 {
					continue
				}
				count1++
				wig[pos]++
			}

			// loop on reads in db2.
			visited := make(map[int]bool)
			rows2, err := readsStmt2.Queryx(ori, j.ref.Name())
			if err != nil {
				log.Fatal(err)
			}
			for rows2.Next() {
				if err = rows2.StructScan(&r); err != nil {
					log.Fatal(err)
				}
				pos := getPos2(&r, ori)
				if visited[pos] && j.opts.Collapse2 {
					continue
				}
				visited[pos] = true
				count2++
				for relPos := -j.opts.Span; relPos <= j.opts.Span; relPos++ {
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

func readRefs(
	db1, db2 *sqlx.DB, decors1, decors2 []BuilderDecorator) ([]feat.Feature, error) {

	var refs []feat.Feature

	// select reference features
	refsB1 := DecorateBuilder(htsdb.ReferenceBuilder, decors1...)
	refs1, err := htsdb.SelectReferences(db1, refsB1)
	if err != nil {
		log.Fatal(err)
	}
	refsB2 := DecorateBuilder(htsdb.ReferenceBuilder, decors2...)
	refs2, err := htsdb.SelectReferences(db2, refsB2)
	if err != nil {
		log.Fatal(err)
	}

	refsmap := make(map[string]htsdb.Reference)
	for _, r := range append(refs1, refs2...) {
		refsmap[r.Chrom] = r
	}

	for k := range refsmap {
		f := refsmap[k]
		refs = append(refs, &f)
	}

	return refs, nil
}

type job struct {
	opts             Opts
	ref              feat.Feature
	decors1, decors2 []BuilderDecorator
	db1, db2         *sqlx.DB
}

type result struct {
	hist   map[int]uint
	count1 int
	count2 int
	job    job
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
