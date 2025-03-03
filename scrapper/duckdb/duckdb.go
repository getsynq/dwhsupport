package duckdb

import (
	"context"

	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	duckdb "github.com/marcboeker/go-duckdb"
	"github.com/pkg/errors"
)

type DuckDBScapperConf = dwhexecduckdb.DuckDBConf

var _ scrapper.Scrapper = &DuckDBScrapper{}

type DuckDBScrapper struct {
	conf     *DuckDBScapperConf
	executor *dwhexecduckdb.DuckDBExecutor
}

func NewDuckDBScrapper(ctx context.Context, conf *DuckDBScapperConf) (*DuckDBScrapper, error) {
	executor, err := dwhexecduckdb.NewDuckDBExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &DuckDBScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *DuckDBScrapper) IsPermissionError(err error) bool {
	duckdbError := &duckdb.Error{}
	if errors.As(err, &duckdbError) {
		return duckdbError.Type == duckdb.ErrorTypePermission
	}
	return false
}

func (e *DuckDBScrapper) Dialect() string {
	return "duckdb"
}

func (e *DuckDBScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *DuckDBScrapper) Close() error {
	return e.executor.Close()
}
