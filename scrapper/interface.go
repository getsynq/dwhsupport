package scrapper

import (
	"context"
	"time"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
)

type NoEnoughPermissionsError struct {
	Err error
}

func (r *NoEnoughPermissionsError) Error() string {
	return errors.Wrap(r.Err, "connection error").Error()
}

func NewNoEnoughPermissionsError(err error) *NoEnoughPermissionsError {
	return &NoEnoughPermissionsError{Err: err}
}

var ErrUnsupported = errors.New("unsupported")

// QueryTablesConfig holds options for QueryTables.
type QueryTablesConfig struct {
	IncludeConstraints bool
}

// QueryTablesOption configures QueryTables behavior.
type QueryTablesOption func(*QueryTablesConfig)

// WithConstraints instructs QueryTables to include table constraints
// (partitioning, clustering, etc.) in the returned TableRow.Constraints field.
// This avoids the need for a separate QueryTableConstraints call when the
// scrapper already fetches the necessary metadata as part of QueryTables.
func WithConstraints() QueryTablesOption {
	return func(c *QueryTablesConfig) {
		c.IncludeConstraints = true
	}
}

// ApplyQueryTablesOptions applies the given options to a QueryTablesConfig.
func ApplyQueryTablesOptions(opts ...QueryTablesOption) QueryTablesConfig {
	var cfg QueryTablesConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Capabilities describes what a scrapper supports beyond the base interface.
// Callers can use this to skip redundant calls (e.g., skip QueryTableConstraints
// when constraints are already provided via QueryTables).
type Capabilities struct {
	// ConstraintsViaQueryTables indicates that QueryTables with WithConstraints()
	// populates TableRow.Constraints, making a separate QueryTableConstraints call unnecessary.
	ConstraintsViaQueryTables bool
}

// Unwrapper is implemented by Scrapper decorators (sanitize, reject, scope, ...)
// so callers can walk to an inner scrapper of a specific type. It mirrors the
// errors.Unwrap idiom: together with As it lets code type-assert past arbitrary
// decorator chains without each decorator having to know about every interface
// a consumer might care about.
type Unwrapper interface {
	Unwrap() Scrapper
}

// As walks the decorator chain rooted at s (following Unwrap) and returns the
// first value that type-asserts to T. Returns the zero T and false if no value
// in the chain satisfies T. Safe to call with a nil Scrapper.
func As[T any](s Scrapper) (T, bool) {
	for s != nil {
		if v, ok := s.(T); ok {
			return v, true
		}
		u, ok := s.(Unwrapper)
		if !ok {
			break
		}
		s = u.Unwrap()
	}
	var zero T
	return zero, false
}

type Scrapper interface {
	DialectType() string
	SqlDialect() sqldialect.Dialect
	IsPermissionError(err error) bool
	// Capabilities returns what this scrapper supports beyond the base interface.
	Capabilities() Capabilities
	ValidateConfiguration(ctx context.Context) (warnings []string, err error)
	QueryCatalog(ctx context.Context) ([]*CatalogColumnRow, error)
	QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*TableMetricsRow, error)
	QuerySqlDefinitions(ctx context.Context) ([]*SqlDefinitionRow, error)
	QueryTables(ctx context.Context, opts ...QueryTablesOption) ([]*TableRow, error)
	QueryDatabases(ctx context.Context) ([]*DatabaseRow, error)
	QuerySegments(ctx context.Context, sql string, args ...any) ([]*SegmentRow, error)
	QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*CustomMetricsRow, error)
	QueryShape(ctx context.Context, sql string) ([]*QueryShapeColumn, error)
	QueryTableConstraints(ctx context.Context) ([]*TableConstraintRow, error)
	// This will close underlying execer, such scrapper can't be used anymore
	Close() error
}

//
// METRICS
//
