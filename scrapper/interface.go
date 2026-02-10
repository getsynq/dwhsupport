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

type Scrapper interface {
	DialectType() string
	SqlDialect() sqldialect.Dialect
	IsPermissionError(err error) bool
	ValidateConfiguration(ctx context.Context) (warnings []string, err error)
	QueryCatalog(ctx context.Context) ([]*CatalogColumnRow, error)
	QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*TableMetricsRow, error)
	QuerySqlDefinitions(ctx context.Context) ([]*SqlDefinitionRow, error)
	QueryTables(ctx context.Context) ([]*TableRow, error)
	QueryDatabases(ctx context.Context) ([]*DatabaseRow, error)
	QuerySegments(ctx context.Context, sql string, args ...any) ([]*SegmentRow, error)
	QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*CustomMetricsRow, error)
	QueryShape(ctx context.Context, sql string) ([]*QueryShapeColumn, error)
	// This will close underlying execer, such scrapper can't be used anymore
	Close() error
}

//
// METRICS
//
