package htsdb

import (
	"github.com/Masterminds/squirrel"
	"github.com/biogo/biogo/feat"
)

// Assert that interfaces are satisfied
var (
	_ feat.Orienter = (*Feature)(nil)
	_ feat.Feature  = (*Feature)(nil)
)

// RangeBuilder is a squirrel select builder whose columns match Range fields.
var RangeBuilder = squirrel.Select("start", "stop", "copy_number")

// Range is part of an htsdb record that wraps the alignment coordinates.
type Range struct {
	StartPos   int `db:"start"`
	StopPos    int `db:"stop"`
	CopyNumber int `db:"copy_number"`
}

// Start returns the start position of Range.
func (e *Range) Start() int { return e.StartPos }

// End returns the end position of Range.
func (e *Range) End() int { return e.StopPos + 1 }

// Len returns the length of Range.
func (e *Range) Len() int { return e.End() - e.Start() }

// CopyNum returns the copy number of Range.
func (e *Range) CopyNum() int { return e.CopyNumber }

// Feature is part of an htsdb record that wraps Range, strand and the name of
// the reference sequence.
type Feature struct {
	Strand int    `db:"strand"`
	Rname  string `db:"rname"`
	Range
}

// Name returns an empty string.
func (e *Feature) Name() string { return "" }

// Orientation returns the orientation of Feature.
func (e *Feature) Orientation() feat.Orientation {
	return feat.Orientation(e.Strand)
}

// Description returns an empty string.
func (e *Feature) Description() string { return "" }

// Location returns the location of Feature.
func (e *Feature) Location() feat.Feature {
	return &Reference{Chrom: e.Rname}
}

// SAM corresponds to database record that has all the fields of a SAM record.
type SAM struct {
	Feature
	Qname string
	Flag  uint
	Pos   uint
	Mapq  uint
	Cigar string
	Rnext string
	Pnext uint
	Tlen  uint
	Seq   string
	Qual  string
	Tags  string
}

// Name returns the SAM qname.
func (e *SAM) Name() string { return e.Qname }

// Head returns the head coordinate of r depending on orientation.
func Head(r feat.Range, o feat.Orientation) int {
	if o == feat.Forward {
		return r.Start()
	} else if o == feat.Reverse {
		return r.End() - 1
	}
	panic("htsdb: strand is not 1 or -1")
}

// Tail returns the tail coordinate of r depending on orientation.
func Tail(r feat.Range, o feat.Orientation) int {
	if o == feat.Forward {
		return r.End() - 1
	} else if o == feat.Reverse {
		return r.Start()
	}
	panic("htsdb: strand is not 1 or -1")
}
