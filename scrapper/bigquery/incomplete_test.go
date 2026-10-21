package bigquery

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/suite"
)

// IncompleteSuite puts the scrapper in front of a project whose "locked"
// dataset refuses its table listing, beside a "sales" dataset in which one
// table refuses its metadata ("secret"), one was dropped after the listing
// ("dropped") and the others answer.
type IncompleteSuite struct {
	suite.Suite
}

func TestIncompleteSuite(t *testing.T) {
	suite.Run(t, new(IncompleteSuite))
}

func salesDataset(extra ...fakeTable) fakeDataset {
	return fakeDataset{
		id: "sales",
		tables: append([]fakeTable{
			fakeBaseTable("orders"),
			fakeView("orders_view"),
			fakeRefusedTable("secret", http.StatusForbidden),
			fakeRefusedTable("dropped", http.StatusNotFound),
		}, extra...),
	}
}

var lockedDataset = fakeDataset{id: "locked", listStatus: http.StatusForbidden}

// methods runs every metadata method that reads each dataset and table, and
// returns the tables it returned rows for.
var methods = map[string]struct {
	part scrapper.OmittedPart
	run  func(ctx context.Context, s scrapper.Scrapper) ([]string, error)
}{
	"QueryTables": {scrapper.OmittedObjects, func(ctx context.Context, s scrapper.Scrapper) ([]string, error) {
		rows, err := s.QueryTables(ctx)
		var out []string
		for _, r := range rows {
			out = append(out, r.Schema+"."+r.Table)
		}
		return out, err
	}},
	"QueryCatalog": {scrapper.OmittedColumns, func(ctx context.Context, s scrapper.Scrapper) ([]string, error) {
		rows, err := s.QueryCatalog(ctx)
		var out []string
		for _, r := range rows {
			out = append(out, r.Schema+"."+r.Table)
		}
		return out, err
	}},
	"QueryTableMetrics": {scrapper.OmittedMetrics, func(ctx context.Context, s scrapper.Scrapper) ([]string, error) {
		rows, err := s.QueryTableMetrics(ctx, time.Time{})
		var out []string
		for _, r := range rows {
			out = append(out, r.Schema+"."+r.Table)
		}
		return out, err
	}},
	"QuerySqlDefinitions": {scrapper.OmittedDefinitions, func(ctx context.Context, s scrapper.Scrapper) ([]string, error) {
		rows, err := s.QuerySqlDefinitions(ctx)
		var out []string
		for _, r := range rows {
			out = append(out, r.Schema+"."+r.Table)
		}
		return out, err
	}},
}

// readTables is what each method returns for salesDataset: the definitions
// listing holds only the view.
var readTables = map[string][]string{
	"QueryTables":         {"sales.orders", "sales.orders_view"},
	"QueryCatalog":        {"sales.orders", "sales.orders_view"},
	"QueryTableMetrics":   {"sales.orders", "sales.orders_view"},
	"QuerySqlDefinitions": {"sales.orders_view"},
}

// withFallback adds what QuerySqlDefinitions reports over the other methods: the
// fake refuses every query job, so the definitions are read through the tables
// API, which has no DDL of a base table, and each base table read that way is
// reported as missing its definition.
func withFallback(name string, scopes []string, baseTables ...string) []string {
	out := append([]string(nil), scopes...)
	if name == "QuerySqlDefinitions" {
		for _, table := range baseTables {
			out = append(out, string(scrapper.OmittedDefinitions)+" test-project."+table)
		}
	}
	return sortedStrings(out)
}

func omissionScopes(err error) []string {
	incomplete, ok := scrapper.AsIncomplete(err)
	if !ok {
		return nil
	}
	var out []string
	for _, o := range incomplete.Omissions {
		out = append(out, string(o.Part)+" "+o.Scope())
	}
	return sortedStrings(out)
}

func (s *IncompleteSuite) TestRefusedDatasetAndTableAreOmitted() {
	for name, m := range methods {
		s.Run(name, func() {
			bq := newFakeMetadataScrapper(s.T(), salesDataset(), lockedDataset)
			tables, err := m.run(scrapper.WithIncompleteResults(context.Background()), bq)

			s.Require().Error(err)
			_, ok := scrapper.AsIncomplete(err)
			s.Require().True(ok, "expected an incomplete result, got %v", err)
			s.Equal(readTables[name], sortedStrings(tables))
			s.Equal(withFallback(name, []string{
				string(m.part) + " test-project.locked",
				string(m.part) + " test-project.sales.secret",
			}, "sales.orders"), omissionScopes(err), "a dropped table is not an omission")
		})
	}
}

func (s *IncompleteSuite) TestRefusedDatasetMetadataIsOmitted() {
	refused := fakeDataset{id: "refused", metadataStatus: http.StatusForbidden, tables: []fakeTable{fakeBaseTable("t")}}
	for _, name := range []string{"QueryTables", "QueryCatalog"} {
		s.Run(name, func() {
			bq := newFakeMetadataScrapper(s.T(), salesDataset(), refused)
			tables, err := methods[name].run(scrapper.WithIncompleteResults(context.Background()), bq)

			s.Equal(readTables[name], sortedStrings(tables))
			s.Equal([]string{
				string(methods[name].part) + " test-project.refused",
				string(methods[name].part) + " test-project.sales.secret",
			}, omissionScopes(err))
		})
	}
}

func (s *IncompleteSuite) TestFailingTableIsOmitted() {
	for name, m := range methods {
		s.Run(name, func() {
			bq := newFakeMetadataScrapper(s.T(), salesDataset(fakeRefusedTable("broken", http.StatusInternalServerError)))
			tables, err := m.run(scrapper.WithIncompleteResults(context.Background()), bq)

			s.Equal(readTables[name], sortedStrings(tables))
			s.Equal(withFallback(name, []string{
				string(m.part) + " test-project.sales.broken",
				string(m.part) + " test-project.sales.secret",
			}, "sales.orders"), omissionScopes(err))
		})
	}
}

func (s *IncompleteSuite) TestEmptyDatasetBesideRefusedOneIsARead() {
	for name, m := range methods {
		s.Run(name, func() {
			bq := newFakeMetadataScrapper(s.T(), fakeDataset{id: "empty"}, lockedDataset)
			tables, err := m.run(scrapper.WithIncompleteResults(context.Background()), bq)

			s.Empty(tables)
			s.Equal([]string{string(m.part) + " test-project.locked"}, omissionScopes(err))
		})
	}
}

func (s *IncompleteSuite) TestNothingReadIsAPlainError() {
	cases := map[string][]fakeDataset{
		"every dataset refused": {lockedDataset},
		"every table refused": {{id: "sales", tables: []fakeTable{
			fakeRefusedTable("secret", http.StatusForbidden),
			fakeRefusedTable("dropped", http.StatusNotFound),
		}}},
	}
	for caseName, datasets := range cases {
		for name, m := range methods {
			s.Run(caseName+"/"+name, func() {
				bq := newFakeMetadataScrapper(s.T(), datasets...)
				tables, err := m.run(scrapper.WithIncompleteResults(context.Background()), bq)

				s.Require().Error(err)
				_, ok := scrapper.AsIncomplete(err)
				s.False(ok, "reading nothing is not a partial result: %v", err)
				s.Empty(tables)
			})
		}
	}
}

func (s *IncompleteSuite) TestDroppedTableIsNotAnOmission() {
	for name, m := range methods {
		s.Run(name, func() {
			bq := newFakeMetadataScrapper(s.T(), fakeDataset{id: "sales", tables: []fakeTable{
				fakeBaseTable("orders"),
				fakeView("orders_view"),
				fakeRefusedTable("dropped", http.StatusNotFound),
			}})
			tables, err := m.run(scrapper.WithIncompleteResults(context.Background()), bq)

			s.Equal(withFallback(name, nil, "sales.orders"), omissionScopes(err))
			s.False(scrapper.Failed(err), "%v", err)
			s.Equal(readTables[name], sortedStrings(tables))
		})
	}
}

func (s *IncompleteSuite) TestShardedTableIsOmittedUnderItsCollapsedName() {
	day := time.Now().UTC().AddDate(0, 0, -2)
	older, latest := day.AddDate(0, 0, -1).Format("20060102"), day.Format("20060102")
	for name, m := range methods {
		s.Run(name, func() {
			bq := newFakeMetadataScrapper(s.T(), fakeDataset{id: "sales", tables: []fakeTable{
				fakeBaseTable("orders"),
				fakeView("orders_view"),
				fakeBaseTable("events_" + older),
				fakeRefusedTable("events_"+latest, http.StatusForbidden),
			}})
			_, err := m.run(scrapper.WithIncompleteResults(context.Background()), bq)
			s.Equal(withFallback(name, []string{string(m.part) + " test-project.sales.events"}, "sales.orders"), omissionScopes(err))
		})
	}
}

func (s *IncompleteSuite) TestNotAcceptingIncompleteKeepsOldBehaviour() {
	for name, m := range methods {
		s.Run(name+"/refused dataset fails the call", func() {
			bq := newFakeMetadataScrapper(s.T(), salesDataset(), lockedDataset)
			tables, err := m.run(context.Background(), bq)

			s.Require().Error(err)
			s.Empty(tables)
			_, ok := scrapper.AsIncomplete(err)
			s.False(ok)
		})
		s.Run(name+"/refused table is skipped", func() {
			bq := newFakeMetadataScrapper(s.T(), salesDataset())
			tables, err := m.run(context.Background(), bq)

			s.Require().NoError(err)
			s.Equal(readTables[name], sortedStrings(tables))
		})
		s.Run(name+"/failing table fails the call", func() {
			bq := newFakeMetadataScrapper(s.T(), salesDataset(fakeRefusedTable("broken", http.StatusInternalServerError)))
			tables, err := m.run(context.Background(), bq)

			s.Require().Error(err)
			s.Empty(tables)
		})
	}
}

// The wrappers a caller stacks over a scrapper pass an incomplete result on
// whole, rows and omissions both.
func (s *IncompleteSuite) TestWrappersPassIncompleteResultsThrough() {
	ctx := scrapper.WithIncompleteResults(context.Background())
	inner := newFakeMetadataScrapper(s.T(), salesDataset(), lockedDataset, fakeDataset{id: "other", tables: []fakeTable{fakeView("t")}})
	wrapped := scope.NewScopedScrapper(
		sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(inner)),
		&scope.ScopeFilter{Exclude: []scope.ScopeRule{{Database: fakeProject, Schema: "other"}}},
	)

	for name, m := range methods {
		s.Run(name, func() {
			tables, err := m.run(ctx, wrapped)
			s.False(scrapper.Failed(err), "%v", err)
			s.Equal(readTables[name], sortedStrings(tables))
			s.Equal(withFallback(name, []string{
				string(m.part) + " test-project.locked",
				string(m.part) + " test-project.sales.secret",
			}, "sales.orders"), omissionScopes(err))
		})
	}
}
