package trino

import (
	"context"
	"time"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type TrinoScrapperConf = dwhexectrino.TrinoConf

var _ scrapper.Scrapper = &TrinoScrapper{}

type TrinoScrapper struct {
	conf     *TrinoScrapperConf
	executor *dwhexectrino.TrinoExecutor
}

func NewTrinoScrapper(ctx context.Context, conf *TrinoScrapperConf) (*TrinoScrapper, error) {
	executor, err := dwhexectrino.NewTrinoExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &TrinoScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *TrinoScrapper) IsPermissionError(err error) bool {
	// TODO: Implement Trino-specific error check
	return false
}

func (e *TrinoScrapper) DialectType() string {
	return "trino"
}

func (e *TrinoScrapper) SqlDialect() sqldialect.Dialect {
	// TODO: Implement or use Trino dialect
	return nil
}

func (e *TrinoScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *TrinoScrapper) Close() error {
	return e.executor.Close()
}

func (e *TrinoScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return nil, scrapper.ErrUnsupported
}

func (e *TrinoScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return nil, scrapper.ErrUnsupported
}

func (e *TrinoScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return nil, scrapper.ErrUnsupported
}

func (e *TrinoScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}

func (e *TrinoScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	return nil, scrapper.ErrUnsupported
}

func (e *TrinoScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return nil, scrapper.ErrUnsupported
}
