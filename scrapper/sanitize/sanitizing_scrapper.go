// Package sanitize provides a Scrapper decorator that strips NUL bytes and
// repairs invalid UTF-8 in every row returned by the underlying scrapper.
// Downstream systems (Postgres text columns, JSON encoders, protobuf text fields)
// reject these byte sequences, so every scrapper should be wrapped at construction.
package sanitize

import (
	"context"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type SanitizingScrapper struct {
	inner scrapper.Scrapper
}

// NewSanitizingScrapper wraps inner so that every returned row has its string
// fields cleaned via each row type's Sanitize() method. Clean strings are returned
// unchanged with zero allocation, so the overhead on well-formed data is negligible.
func NewSanitizingScrapper(inner scrapper.Scrapper) *SanitizingScrapper {
	return &SanitizingScrapper{inner: inner}
}

func (s *SanitizingScrapper) Capabilities() scrapper.Capabilities { return s.inner.Capabilities() }
func (s *SanitizingScrapper) DialectType() string                 { return s.inner.DialectType() }
func (s *SanitizingScrapper) SqlDialect() sqldialect.Dialect      { return s.inner.SqlDialect() }
func (s *SanitizingScrapper) IsPermissionError(err error) bool    { return s.inner.IsPermissionError(err) }
func (s *SanitizingScrapper) Close() error                        { return s.inner.Close() }

func (s *SanitizingScrapper) ValidateConfiguration(ctx context.Context) (warnings []string, err error) {
	warnings, err = s.inner.ValidateConfiguration(ctx)
	for i, w := range warnings {
		warnings[i] = scrapper.SanitizeString(w)
	}
	return warnings, err
}

func (s *SanitizingScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	rows, err := s.inner.QueryCatalog(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	rows, err := s.inner.QueryTableMetrics(ctx, lastMetricsFetchTime)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	rows, err := s.inner.QuerySqlDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	rows, err := s.inner.QueryTables(ctx, opts...)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	rows, err := s.inner.QueryDatabases(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	rows, err := s.inner.QuerySegments(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	rows, err := s.inner.QueryCustomMetrics(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

func (s *SanitizingScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	cols, err := s.inner.QueryShape(ctx, sql)
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		c.Sanitize()
	}
	return cols, nil
}

func (s *SanitizingScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	rows, err := s.inner.QueryTableConstraints(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r.Sanitize()
	}
	return rows, nil
}

var _ scrapper.Scrapper = (*SanitizingScrapper)(nil)
