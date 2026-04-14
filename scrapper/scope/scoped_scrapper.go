package scope

import (
	"context"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// ScopedScrapper wraps a Scrapper with a base ScopeFilter.
// It injects the base scope into every call's context and post-filters results.
// Callers can further narrow the scope via WithScope on the context.
type ScopedScrapper struct {
	inner     scrapper.Scrapper
	baseScope *ScopeFilter
}

// NewScopedScrapper creates a ScopedScrapper wrapping inner with the given base scope.
// If baseScope is nil, the wrapper still implements Scrapper but does no filtering.
func NewScopedScrapper(inner scrapper.Scrapper, baseScope *ScopeFilter) *ScopedScrapper {
	return &ScopedScrapper{inner: inner, baseScope: baseScope}
}

// BaseScope returns the base scope filter configured for this scrapper.
func (s *ScopedScrapper) BaseScope() *ScopeFilter {
	return s.baseScope
}

// Unwrap returns the inner scrapper, letting callers walk the decorator chain
// (via scrapper.As) to reach concrete types or interfaces this wrapper does
// not itself implement.
func (s *ScopedScrapper) Unwrap() scrapper.Scrapper { return s.inner }

func (s *ScopedScrapper) effectiveCtx(ctx context.Context) context.Context {
	return WithScope(ctx, s.baseScope)
}

func (s *ScopedScrapper) effectiveFilter(ctx context.Context) *ScopeFilter {
	return GetScope(s.effectiveCtx(ctx))
}

// Scrapper interface — pass-through methods (not filtered by scope).
// These either don't return warehouse objects (DialectType, SqlDialect, IsPermissionError, Close,
// ValidateConfiguration) or execute user-provided SQL that cannot be scope-filtered
// (QuerySegments, QueryCustomMetrics, QueryShape).

func (s *ScopedScrapper) Capabilities() scrapper.Capabilities { return s.inner.Capabilities() }
func (s *ScopedScrapper) DialectType() string                 { return s.inner.DialectType() }
func (s *ScopedScrapper) SqlDialect() sqldialect.Dialect      { return s.inner.SqlDialect() }
func (s *ScopedScrapper) IsPermissionError(err error) bool    { return s.inner.IsPermissionError(err) }
func (s *ScopedScrapper) Close() error                        { return s.inner.Close() }

func (s *ScopedScrapper) ValidateConfiguration(ctx context.Context) (warnings []string, err error) {
	return s.inner.ValidateConfiguration(ctx)
}

func (s *ScopedScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	return s.inner.QuerySegments(ctx, sql, args...)
}

func (s *ScopedScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return s.inner.QueryCustomMetrics(ctx, sql, args...)
}

func (s *ScopedScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	return s.inner.QueryShape(ctx, sql)
}

// Scrapper interface — filtered methods.
// These inject the base scope into the context (enabling SQL push-down in the inner scrapper)
// and post-filter returned rows to guarantee scope compliance even when the inner scrapper
// does not support SQL push-down.

func (s *ScopedScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	ctx = s.effectiveCtx(ctx)
	rows, err := s.inner.QueryCatalog(ctx)
	if err != nil {
		return nil, err
	}
	return FilterRows(rows, GetScope(ctx)), nil
}

func (s *ScopedScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	ctx = s.effectiveCtx(ctx)
	rows, err := s.inner.QueryTableMetrics(ctx, lastMetricsFetchTime)
	if err != nil {
		return nil, err
	}
	return FilterRows(rows, GetScope(ctx)), nil
}

func (s *ScopedScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	ctx = s.effectiveCtx(ctx)
	rows, err := s.inner.QuerySqlDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	return FilterRows(rows, GetScope(ctx)), nil
}

func (s *ScopedScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	ctx = s.effectiveCtx(ctx)
	rows, err := s.inner.QueryTables(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return FilterRows(rows, GetScope(ctx)), nil
}

func (s *ScopedScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	ctx = s.effectiveCtx(ctx)
	rows, err := s.inner.QueryDatabases(ctx)
	if err != nil {
		return nil, err
	}
	return FilterDatabaseRows(rows, GetScope(ctx)), nil
}

func (s *ScopedScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	ctx = s.effectiveCtx(ctx)
	rows, err := s.inner.QueryTableConstraints(ctx)
	if err != nil {
		return nil, err
	}
	return FilterRows(rows, GetScope(ctx)), nil
}

// Ensure ScopedScrapper implements Scrapper at compile time.
var _ scrapper.Scrapper = (*ScopedScrapper)(nil)
