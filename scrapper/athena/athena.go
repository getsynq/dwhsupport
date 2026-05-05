// Package athena implements the scrapper.Scrapper interface for Amazon Athena.
//
// V1 scope is metadata-only: catalog, tables, databases, view definitions.
// Table metrics, query logs, freshness, table constraints, and table comments
// are deferred — those methods either return ErrUnsupported or empty slices.
//
// Athena exposes a Presto-derived SQL surface, so most queries look like Trino's
// but talk to Athena's information_schema (no system.metadata.* tables, no
// per-catalog prefix needed — Athena's information_schema is already
// catalog-scoped to the data catalog the connection is bound to).
package athena

import (
	"context"
	"strings"

	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type AthenaScrapperConf struct {
	*dwhexecathena.AthenaConf

	// UseShowCreateView, when true, calls `SHOW CREATE VIEW` per view to
	// retrieve the full `CREATE OR REPLACE VIEW … AS …` DDL instead of just
	// the rewritten Presto body from `information_schema.views.view_definition`.
	// Each call is one Athena query (~$0.00005 at the 10MB scan minimum).
	UseShowCreateView bool

	// UseShowCreateTable, when true, calls `SHOW CREATE TABLE` per non-view
	// object to retrieve the table DDL. This is the only way to get table DDL
	// on Athena — `information_schema` exposes no equivalent. Required to see
	// CTAS bodies (lineage), Iceberg `TBLPROPERTIES`, partitioning, and Hive
	// external `LOCATION`/SerDe info. Each call is one Athena query.
	UseShowCreateTable bool

	// Scope is the include/exclude filter for Glue catalog/database/table.
	// Nil means accept-all. The cloud and agent protos both carry this as
	// a synq.common.v1.ScopeFilter; callers translate via ScopeFromProto.
	Scope *scope.ScopeFilter
}

var _ scrapper.Scrapper = &AthenaScrapper{}

type AthenaScrapper struct {
	conf     *AthenaScrapperConf
	executor *dwhexecathena.AthenaExecutor
}

func NewAthenaScrapper(ctx context.Context, conf *AthenaScrapperConf) (*AthenaScrapper, error) {
	executor, err := dwhexecathena.NewAthenaExecutor(ctx, conf.AthenaConf)
	if err != nil {
		return nil, err
	}
	return &AthenaScrapper{conf: conf, executor: executor}, nil
}

func (e *AthenaScrapper) DialectType() string { return "athena" }

func (e *AthenaScrapper) SqlDialect() sqldialect.Dialect {
	// Athena is Presto-derived; the Trino dialect is the closest fit and
	// covers the SQL features we care about (quoting, CAST, INFORMATION_SCHEMA).
	return sqldialect.NewTrinoDialect()
}

func (e *AthenaScrapper) IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "AccessDenied") ||
		strings.Contains(msg, "not authorized") ||
		strings.Contains(msg, "UnauthorizedOperation")
}

func (e *AthenaScrapper) Capabilities() scrapper.Capabilities {
	return scrapper.Capabilities{}
}

func (e *AthenaScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	// A trivial query that exercises auth + workgroup config + result location.
	var n int
	if err := e.executor.GetDb().GetContext(ctx, &n, "SELECT 1"); err != nil {
		return nil, err
	}
	return nil, nil
}

func (e *AthenaScrapper) Close() error {
	return e.executor.Close()
}
