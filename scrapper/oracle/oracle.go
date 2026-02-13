package oracle

import (
	"context"
	"strings"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type OracleScrapperConf = dwhexecoracle.OracleConf

var _ scrapper.Scrapper = &OracleScrapper{}

type OracleScrapper struct {
	conf     *OracleScrapperConf
	executor *dwhexecoracle.OracleExecutor
}

func NewOracleScrapper(ctx context.Context, conf *OracleScrapperConf) (*OracleScrapper, error) {
	executor, err := dwhexecoracle.NewOracleExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &OracleScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *OracleScrapper) IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// ORA-00942: table or view does not exist
	// ORA-01031: insufficient privileges
	// ORA-00604: error occurred at recursive SQL level (often permission-related)
	return strings.Contains(errStr, "ORA-00942") ||
		strings.Contains(errStr, "ORA-01031") ||
		strings.Contains(errStr, "ORA-00604")
}

func (e *OracleScrapper) DialectType() string {
	return "oracle"
}

func (e *OracleScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewOracleDialect()
}

func (e *OracleScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *OracleScrapper) Close() error {
	return e.executor.Close()
}
