package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"

	"github.com/jmoiron/sqlx"
	"github.com/mnsmar/htsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

const prog = "htsdb-to-sam"
const version = "0.1"
const descr = `Print database records in SAM format. Provided SQL filters will
apply to output.`

var (
	app = kingpin.New(prog, descr)

	dbFile = app.Flag("db", "SQLite file.").
		PlaceHolder("<file>").Required().String()
	tab = app.Flag("table", "Database table name.").
		Default("sample").String()
	where = app.Flag("where", "SQL filter injected in WHERE clause.").
		PlaceHolder("<SQL>").String()
	header = app.Flag("header", "build and print SAM header.").
		Bool()
	verbose = app.Flag("verbose", "Verbose mode.").Short('v').Bool()
)

// Reader encapsulates a connection to a database and implements io.Reader.
type Reader struct {
	db   *sqlx.DB
	dest *htsdb.SamRecord
	rows *sqlx.Rows
	err  error
}

// NewReader returns a new Reader that reads from db using the given query.
func NewReader(db *sqlx.DB, query string) (*Reader, error) {
	rows, err := db.Queryx(query)
	if err != nil {
		return nil, err
	}

	return &Reader{
		db:   db,
		dest: &htsdb.SamRecord{},
		rows: rows,
	}, nil
}

// Read reads the next record from r into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered. Even if Read returns n <
// len(p), it may use all of p as scratch space during the call. If some data
// is available but not len(p) bytes, Read conventionally returns what is
// available instead of waiting for more. It will return n = 0 and io.EOF when
// r is exhausted. It will return n = 0 and an error if it encounters one.
func (r *Reader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	ok := r.rows.Next()
	if !ok {
		if r.rows.Err() == nil {
			return 0, io.EOF
		}
		return 0, r.rows.Err()
	}
	err = r.rows.StructScan(r.dest)
	if err != nil {
		return 0, err
	}
	s := r.dest.Qname + "\t" +
		strconv.Itoa(r.dest.Flag) + "\t" +
		r.dest.Rname + "\t" +
		strconv.Itoa(r.dest.Pos) + "\t" +
		strconv.Itoa(r.dest.Mapq) + "\t" +
		r.dest.Cigar + "\t" +
		r.dest.Rnext + "\t" +
		strconv.Itoa(r.dest.Pnext) + "\t" +
		strconv.Itoa(r.dest.Tlen) + "\t" +
		r.dest.Seq + "\t" +
		r.dest.Qual + "\t" +
		r.dest.Tags + "\n"

	n = copy(p, s[0:])

	return
}

func main() {
	app.HelpFlag.Short('h')
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("%s", err)
	}

	db, err := sqlx.Connect("sqlite3", *dbFile)
	if err != nil {
		log.Fatal(err)
	}

	readsB := htsdb.SamRecordBuilder.From(*tab)
	refsB := htsdb.ReferenceBuilder.From(*tab)
	if *where != "" {
		readsB = readsB.Where(*where)
		refsB = refsB.Where(*where)
	}

	query, _, err := readsB.ToSql()
	if err != nil {
		log.Fatal(err)
	}

	if *header == true {
		refs, err := htsdb.SelectReferences(db, refsB)
		if err != nil {
			log.Fatal(err)
		}
		for _, r := range refs {
			fmt.Printf("@SQ\tSN:%s\tLN:%d\n", r.Name(), r.Len())
		}
	}

	r, err := NewReader(db, query)
	if err != nil {
		log.Fatal(err)
	}

	sc := bufio.NewScanner(r)
	for {
		ok := sc.Scan()
		if ok == false {
			break
		}
		fmt.Printf("%s\n", sc.Text())
	}
	if sc.Err() != nil {
		log.Fatal(err)
	}
}
