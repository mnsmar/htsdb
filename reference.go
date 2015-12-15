package htsdb

import (
	"github.com/biogo/biogo/feat"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// Assert that interfaces are satisfied
var (
	_ feat.Feature = (*Reference)(nil)
)

// ReferenceBuilder is a squirrel select builder whose structure matches that
// of Reference and that knows how to properly extract references from htsdb.
var ReferenceBuilder = squirrel.Select("rname").
	Column(squirrel.Alias(squirrel.Expr("MAX(stop)+1"), "length")).
	GroupBy("rname")

// Reference is a reference feature on which reads align.
type Reference struct {
	Chrom  string `db:"rname"`
	Length int    `db:"length"`
}

// Start returns 0.
func (c *Reference) Start() int { return 0 }

// End returns the end of the reference.
func (c *Reference) End() int { return c.Len() }

// Len returns the length of the reference.
func (c *Reference) Len() int { return c.Length }

// Name returns the name of the reference.
func (c *Reference) Name() string { return c.Chrom }

// Description returns the description of the reference.
func (c *Reference) Description() string { return "reference" }

// Description returns nil.
func (c *Reference) Location() feat.Feature { return nil }

// SelectReferences selects the reference sequences from db using
// squirrel.SelectBuilder. It will return an error if it encounters one.
//
// e.g.
// refs, err := SelectReferences(db, ReferenceBuilder)
func SelectReferences(db *sqlx.DB, b squirrel.SelectBuilder) ([]Reference, error) {
	refs := []Reference{}
	query, _, err := b.ToSql()
	if err != nil {
		return refs, err
	}
	err = db.Select(&refs, query)
	return refs, err
}
