// Package fabric implements a scrapper for Microsoft Fabric Warehouses and
// Lakehouse SQL analytics endpoints.
//
// Fabric speaks T-SQL over TDS, so it shares the MSSQL executor and (mostly) the
// MSSQL dialect. It is a first-class scrapper rather than a thin MSSQL alias
// because Fabric Warehouse's metadata surface diverges meaningfully from SQL
// Server — no page-based storage stats, informational-only NOT ENFORCED
// constraints (including foreign keys), no extended properties, no NVARCHAR — so
// every metadata query here is written against the Fabric surface directly.
package fabric

import (
	"context"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// FabricScrapperConf configures a Fabric scrapper. It embeds the executor conf
// (the connection surface) and adds scrapper-level options — keeping the scrape
// scope out of the connection config.
type FabricScrapperConf struct {
	dwhexecfabric.FabricConf

	// Scope is the configured include/exclude filter over the workspace's
	// databases/schemas/tables (ScopeRule.database = Fabric database/warehouse,
	// .schema = schema, .table = table). It is the canonical scope encoding
	// (synq.common.v1.ScopeFilter, as Athena uses) rather than a legacy database
	// list. nil means the whole workspace. Per-call scope (scope.WithScope) can
	// only narrow within this configured scope, never escape it — see
	// effectiveScope.
	Scope *scope.ScopeFilter
}

var _ scrapper.Scrapper = &FabricScrapper{}

type FabricScrapper struct {
	conf        *FabricScrapperConf
	executor    *dwhexecfabric.FabricExecutor
	existingDbs lazy.Lazy[[]string]
}

// listDatabasesSql lists the workspace databases visible to the connection. The
// SQL endpoint is workspace-shared, so this returns every warehouse/lakehouse
// the principal can see. database_id > 4 skips master/tempdb/model/msdb.
const listDatabasesSql = `
SELECT name FROM sys.databases
WHERE database_id > 4 AND name NOT IN ('master', 'tempdb', 'model', 'msdb')
ORDER BY name`

func NewFabricScrapper(ctx context.Context, conf *FabricScrapperConf) (*FabricScrapper, error) {
	executor, err := dwhexecfabric.NewFabricExecutor(ctx, &conf.FabricConf)
	if err != nil {
		return nil, err
	}

	e := &FabricScrapper{
		conf:     conf,
		executor: executor,
	}
	e.existingDbs = lazy.New(func() ([]string, error) {
		rows, err := dwhexecfabric.NewQuerier[dbNameRow](executor).QueryMany(ctx, listDatabasesSql)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(rows))
		for _, r := range rows {
			names = append(names, r.Name)
		}
		return names, nil
	})
	return e, nil
}

type dbNameRow struct {
	Name string `db:"name"`
}

// GetExistingDbs returns the workspace databases visible to the connection,
// cached after the first call.
func (e *FabricScrapper) GetExistingDbs(ctx context.Context) ([]string, error) {
	return e.existingDbs.Get()
}

// effectiveScope merges the configured scope with any per-call scope injected
// into the context (scope.WithScope). Merge is AND semantics, so the per-call
// scope can only narrow within the configured scope and can never widen beyond
// it — the configured scope is an inescapable boundary. Mirrors the Athena
// scrapper's effectiveScope.
func (e *FabricScrapper) effectiveScope(ctx context.Context) *scope.ScopeFilter {
	return scope.Merge(e.conf.Scope, scope.GetScope(ctx))
}

// withEffectiveScope injects the merged scope into the context so both database
// enumeration (GetDatabasesToQuery) and SQL push-down (AppendScopeConditions)
// honor the configured scope, not just the ScopedScrapper post-filter — so
// excluded databases/tables are never enumerated or scanned in the first place.
func (e *FabricScrapper) withEffectiveScope(ctx context.Context) context.Context {
	return scope.WithScope(ctx, e.effectiveScope(ctx))
}

// GetDatabasesToQuery returns the workspace databases each scrapper method should
// iterate — every database the connection can see (via sys.databases), narrowed
// by the effective (configured AND per-call) scope. This is the workspace-centric
// analogue of the Snowflake scrapper's method of the same name.
func (e *FabricScrapper) GetDatabasesToQuery(ctx context.Context) ([]string, error) {
	all, err := e.GetExistingDbs(ctx)
	if err != nil {
		return nil, err
	}

	scopeFilter := e.effectiveScope(ctx)

	var result []string
	for _, db := range all {
		if scopeFilter.IsDatabaseAccepted(db) {
			result = append(result, db)
		}
	}
	return result, nil
}

func (e *FabricScrapper) Executor() *dwhexecfabric.FabricExecutor {
	return e.executor
}

func (e *FabricScrapper) IsPermissionError(err error) bool {
	return dwhexecfabric.IsPermissionError(err)
}

func (e *FabricScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

func (e *FabricScrapper) DialectType() string {
	return "fabric"
}

func (e *FabricScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewFabricDialect()
}

func (e *FabricScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *FabricScrapper) Close() error {
	return e.executor.Close()
}
