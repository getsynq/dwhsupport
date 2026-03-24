package oracle

import (
	"context"
	"strings"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type OracleScrapperConf struct {
	dwhexecoracle.OracleConf

	// UseDiagnosticsPack enables use of AWR (Automatic Workload Repository) views
	// for query log collection. AWR is part of the Oracle Diagnostics Pack, which
	// requires a separate license for Oracle Database Enterprise Edition.
	// When false (default), query logs come from V$SQL (no additional license needed).
	// V$SQL provides in-memory cached SQL statements that age out under memory pressure.
	// When true, query logs come from DBA_HIST_SQLSTAT + DBA_HIST_SQLTEXT which
	// provides persistent AWR snapshots (typically hourly, retained for days/weeks).
	// Requires Diagnostics Pack license and SELECT ANY DICTIONARY or SELECT_CATALOG_ROLE grant.
	UseDiagnosticsPack bool
}

var _ scrapper.Scrapper = &OracleScrapper{}

type OracleScrapper struct {
	conf     *OracleScrapperConf
	executor *dwhexecoracle.OracleExecutor
}

func NewOracleScrapper(ctx context.Context, conf *OracleScrapperConf) (*OracleScrapper, error) {
	executor, err := dwhexecoracle.NewOracleExecutor(ctx, &conf.OracleConf)
	if err != nil {
		return nil, err
	}

	return &OracleScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *OracleScrapper) Executor() *dwhexecoracle.OracleExecutor {
	return e.executor
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

func (e *OracleScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

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
