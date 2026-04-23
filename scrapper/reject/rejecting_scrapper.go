// Package reject provides a Scrapper decorator that drops rows whose identity
// fields (the parts forming an object's fully-qualified name) contain NUL bytes
// or invalid UTF-8.
//
// Unlike sanitize, which repairs bad strings in place, reject discards the row
// entirely — because a sanitised identifier might collide with another object or
// fail to resolve against the warehouse. For non-identity content (descriptions,
// SQL, tag values), compose with sanitize:
//
//	sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(inner))
package reject

import (
	"context"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/sirupsen/logrus"
)

type RejectingScrapper struct {
	inner scrapper.Scrapper
}

func NewRejectingScrapper(inner scrapper.Scrapper) *RejectingScrapper {
	return &RejectingScrapper{inner: inner}
}

// Unwrap returns the inner scrapper, letting callers walk the decorator chain
// (via scrapper.As) to reach concrete types or interfaces this wrapper does
// not itself implement.
func (s *RejectingScrapper) Unwrap() scrapper.Scrapper { return s.inner }

func (s *RejectingScrapper) Capabilities() scrapper.Capabilities { return s.inner.Capabilities() }
func (s *RejectingScrapper) DialectType() string                 { return s.inner.DialectType() }
func (s *RejectingScrapper) SqlDialect() sqldialect.Dialect      { return s.inner.SqlDialect() }
func (s *RejectingScrapper) IsPermissionError(err error) bool    { return s.inner.IsPermissionError(err) }
func (s *RejectingScrapper) Close() error                        { return s.inner.Close() }

func (s *RejectingScrapper) ValidateConfiguration(ctx context.Context) (warnings []string, err error) {
	return s.inner.ValidateConfiguration(ctx)
}

type identified interface {
	HasValidIdentity() bool
}

// filterValid returns only the entries with valid identity. Drops are logged
// at Warn level with the scrapper's dialect and the calling method for traceability.
func filterValid[T identified](rows []T, dialect, method string) []T {
	kept := rows[:0]
	dropped := 0
	for _, r := range rows {
		if r.HasValidIdentity() {
			kept = append(kept, r)
			continue
		}
		dropped++
	}
	if dropped > 0 {
		logrus.WithFields(logrus.Fields{
			"dialect": dialect,
			"method":  method,
			"dropped": dropped,
			"kept":    len(kept),
		}).Warn("rejected rows with invalid identity fields (NUL bytes or invalid UTF-8)")
	}
	return kept
}

func (s *RejectingScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	rows, err := s.inner.QueryCatalog(ctx)
	if err != nil {
		return nil, err
	}
	rows = filterValid(rows, s.DialectType(), "QueryCatalog")
	for _, r := range rows {
		r.ColumnTags = filterValid(r.ColumnTags, s.DialectType(), "QueryCatalog.ColumnTags")
		r.TableTags = filterValid(r.TableTags, s.DialectType(), "QueryCatalog.TableTags")
		r.FieldSchemas = filterValid(r.FieldSchemas, s.DialectType(), "QueryCatalog.FieldSchemas")
	}
	return rows, nil
}

func (s *RejectingScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	rows, err := s.inner.QueryTableMetrics(ctx, lastMetricsFetchTime)
	if err != nil {
		return nil, err
	}
	return filterValid(rows, s.DialectType(), "QueryTableMetrics"), nil
}

func (s *RejectingScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	rows, err := s.inner.QuerySqlDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	rows = filterValid(rows, s.DialectType(), "QuerySqlDefinitions")
	for _, r := range rows {
		r.Tags = filterValid(r.Tags, s.DialectType(), "QuerySqlDefinitions.Tags")
	}
	return rows, nil
}

func (s *RejectingScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	rows, err := s.inner.QueryTables(ctx, opts...)
	if err != nil {
		return nil, err
	}
	rows = filterValid(rows, s.DialectType(), "QueryTables")
	for _, r := range rows {
		r.Tags = filterValid(r.Tags, s.DialectType(), "QueryTables.Tags")
		r.Annotations = filterValid(r.Annotations, s.DialectType(), "QueryTables.Annotations")
		r.Constraints = filterValid(r.Constraints, s.DialectType(), "QueryTables.Constraints")
	}
	return rows, nil
}

func (s *RejectingScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	rows, err := s.inner.QueryDatabases(ctx)
	if err != nil {
		return nil, err
	}
	return filterValid(rows, s.DialectType(), "QueryDatabases"), nil
}

func (s *RejectingScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	rows, err := s.inner.QuerySegments(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return filterValid(rows, s.DialectType(), "QuerySegments"), nil
}

func (s *RejectingScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	rows, err := s.inner.QueryCustomMetrics(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return filterValid(rows, s.DialectType(), "QueryCustomMetrics"), nil
}

func (s *RejectingScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	cols, err := s.inner.QueryShape(ctx, sql)
	if err != nil {
		return nil, err
	}
	return filterValid(cols, s.DialectType(), "QueryShape"), nil
}

func (s *RejectingScrapper) RunRawQuery(ctx context.Context, sql string) (scrapper.RawQueryRowIterator, error) {
	return s.inner.RunRawQuery(ctx, sql)
}

func (s *RejectingScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	rows, err := s.inner.QueryTableConstraints(ctx)
	if err != nil {
		return nil, err
	}
	return filterValid(rows, s.DialectType(), "QueryTableConstraints"), nil
}

var _ scrapper.Scrapper = (*RejectingScrapper)(nil)
