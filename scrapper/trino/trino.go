package trino

import (
	"context"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type TrinoScrapperConf struct {
	*dwhexectrino.TrinoConf
	Catalogs           []string
	UseShowCreateView  bool
	UseShowCreateTable bool
}

var _ scrapper.Scrapper = &TrinoScrapper{}

type TrinoScrapper struct {
	conf     *TrinoScrapperConf
	executor *dwhexectrino.TrinoExecutor
}

func NewTrinoScrapper(ctx context.Context, conf *TrinoScrapperConf) (*TrinoScrapper, error) {
	executor, err := dwhexectrino.NewTrinoExecutor(ctx, conf.TrinoConf)
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

func (e *TrinoScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
