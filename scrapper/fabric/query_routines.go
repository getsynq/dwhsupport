package fabric

import (
	"context"
	_ "embed"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_routines.sql
var queryRoutinesSql string

// QueryRoutines lists every in-scope database's routines. A database that
// fails fails the whole listing.
func (e *FabricScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	ctx = e.withEffectiveScope(ctx)
	return queryEachDatabase(ctx, e, wholeListing,
		func(db string) string {
			return expandDatabase(scope.AppendScopeConditions(ctx, queryRoutinesSql, "", "s.name", "o.name"), db)
		},
		func(row *scrapper.RoutineRow, db string) {
			row.Instance = e.conf.Host
			row.Database = db
		},
	)
}
