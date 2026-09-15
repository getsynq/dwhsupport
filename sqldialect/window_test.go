package sqldialect

import (
	"strings"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type WindowSuite struct {
	suite.Suite
}

func TestWindowSuite(t *testing.T) {
	suite.Run(t, new(WindowSuite))
}

func (s *WindowSuite) snapshotEach(dir string, build func(Dialect) Expr) {
	s.T().Helper()

	for _, dialect := range DialectsToTest() {
		sql, err := build(dialect.Dialect).ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir(dir), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *WindowSuite) TestRowNumberOverPartition() {
	s.snapshotEach("RowNumberOverPartition", func(Dialect) Expr {
		return Over(RowNumber()).
			PartitionBy(TextCol("category")).
			OrderBy(Asc(TextCol("id")))
	})
}

func (s *WindowSuite) TestRowNumberOverEmptyWindow() {
	s.snapshotEach("RowNumberOverEmptyWindow", func(Dialect) Expr {
		return Over(RowNumber())
	})
}

func (s *WindowSuite) TestNtileOverOrderBy() {
	s.snapshotEach("NtileOverOrderBy", func(Dialect) Expr {
		return Over(Ntile(Int64(16))).OrderBy(Asc(TextCol("id")))
	})
}

func (s *WindowSuite) TestNtileOverPartitionAndDescOrder() {
	s.snapshotEach("NtileOverPartitionAndDescOrder", func(Dialect) Expr {
		return Over(Ntile(Int64(8))).
			PartitionBy(TextCol("category"), TextCol("region")).
			OrderBy(Desc(TextCol("id")), Asc(TextCol("created_at")))
	})
}

// An alias and a reference to it must be spelled the same way. Identifier
// quotes unconditionally on the dialects whose Identifier is a quoting
// function, so an alias built with it would not match the reference.
func (s *WindowSuite) TestAliasMatchesReference() {
	for _, dialect := range DialectsToTest() {
		alias, err := Alias("keyVal").ToSql(dialect.Dialect)
		s.Require().NoError(err)

		ref, err := QualifiedCol("t", "keyVal").ToSql(dialect.Dialect)
		s.Require().NoError(err)

		s.Equal(alias, strings.TrimPrefix(ref, ref[:strings.LastIndex(ref, ".")+1]),
			"%s: alias %q and reference %q must agree", dialect.Name, alias, ref)
	}
}

func (s *WindowSuite) TestAlias() {
	s.snapshotEach("Alias", func(Dialect) Expr {
		return As(TextCol("id"), Alias("key_val"))
	})
}

func (s *WindowSuite) TestAliasMixedCase() {
	s.snapshotEach("AliasMixedCase", func(Dialect) Expr {
		return As(TextCol("id"), Alias("keyVal"))
	})
}

func (s *WindowSuite) TestQualifiedCol() {
	s.snapshotEach("QualifiedCol", func(Dialect) Expr {
		return QualifiedCol("_recon_base", "key_val")
	})
}

// A mixed-case alias or column has to come back quoted on every dialect that
// folds, or the reference misses the thing it names.
func (s *WindowSuite) TestQualifiedColMixedCase() {
	s.snapshotEach("QualifiedColMixedCase", func(Dialect) Expr {
		return QualifiedCol("reconBase", "keyVal")
	})
}

// The checkpoint query reconciliation builds: NTILE over the ordered key inside
// a derived table, then each bucket's bounds read back through the alias.
func (s *WindowSuite) TestNtileCheckpointQuery() {
	for _, dialect := range DialectsToTest() {
		bucketed := NewSelect().
			From(SubqueryTable("select id, name from products", "_recon_base")).
			Cols(
				As(TextCol("id"), Alias("key_val")),
				As(Over(Ntile(Int64(4))).OrderBy(Asc(TextCol("id"))), Alias("bucket")),
			)

		bucketedSql, err := bucketed.ToSql(dialect.Dialect)
		s.Require().NoError(err)

		sel := NewSelect().
			From(SubqueryTable(bucketedSql, "_recon_bucketed")).
			Cols(
				As(QualifiedCol("_recon_bucketed", "bucket"), Alias("bucket")),
				As(Fn("MIN", QualifiedCol("_recon_bucketed", "key_val")), Alias("lo")),
				As(Fn("MAX", QualifiedCol("_recon_bucketed", "key_val")), Alias("hi")),
				As(CountAll(), Alias("cnt")),
			).
			GroupBy(QualifiedCol("_recon_bucketed", "bucket")).
			OrderBy(Asc(QualifiedCol("_recon_bucketed", "bucket")))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("NtileCheckpointQuery"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
