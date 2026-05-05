package athena

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"golang.org/x/sync/errgroup"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSQL string

// QuerySqlDefinitions returns view (and optionally table) DDLs.
//
// Default: pulls view bodies from information_schema.views.view_definition —
// one bulk query, free (DDL).
//
// With UseShowCreateView/UseShowCreateTable enabled, additionally calls
// SHOW CREATE VIEW / SHOW CREATE TABLE per object in an 8-way errgroup pool.
// SHOW CREATE on Athena is the only way to retrieve table DDL (CTAS bodies,
// Iceberg TBLPROPERTIES, Hive external LOCATION/SerDe) and the only way to
// get the full CREATE OR REPLACE VIEW DDL with original column declarations.
// Each call is one Athena query execution (~$0.00005 at the 10MB scan minimum).
func (e *AthenaScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	basic, err := e.getBasicSqlDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	if len(basic) > 0 && (e.conf.UseShowCreateTable || e.conf.UseShowCreateView) {
		g, groupCtx := errgroup.WithContext(ctx)
		g.SetLimit(8)
		for i, row := range basic {
			if groupCtx.Err() != nil {
				break
			}
			if i > 0 && i%100 == 0 {
				logging.GetLogger(groupCtx).Infof("athena: fetched SHOW CREATE for %d/%d", i, len(basic))
			}

			wantView := row.IsView && e.conf.UseShowCreateView
			wantTable := !row.IsView && e.conf.UseShowCreateTable
			if !wantView && !wantTable {
				continue
			}

			g.Go(func() error {
				ddl, err := e.showCreate(groupCtx, row.Database, row.Schema, row.Table, row.IsView)
				if err != nil {
					logging.GetLogger(groupCtx).
						WithField("table_fqn", row.TableFqn()).
						WithError(err).
						Warnf("athena: SHOW CREATE failed")
					return nil
				}
				if ddl != "" {
					row.Sql = ddl
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}

	out := basic[:0]
	for _, r := range basic {
		if r.Sql != "" {
			out = append(out, r)
		}
	}
	return out, nil
}

func (e *AthenaScrapper) showCreate(ctx context.Context, database, schema, table string, isView bool) (string, error) {
	// Athena quirks vs. Trino:
	//   - The catalog prefix is rejected on SHOW CREATE — the connection is
	//     already bound to a single Glue Data Catalog.
	//   - Quoting is asymmetric:
	//       SHOW CREATE TABLE — backticks ONLY (ANSI quotes → "Queries of this type are not supported")
	//       SHOW CREATE VIEW  — ANSI quotes ONLY (backticks → "backquoted identifiers are not supported")
	//   - SHOW CREATE TABLE on Hive external tables probes the `default` Glue
	//     database internally; the caller's IAM principal needs glue:GetDatabase
	//     on `default` even if no real data lives there.
	var query string
	if isView {
		query = fmt.Sprintf(`SHOW CREATE VIEW "%s"."%s"`, schema, table)
	} else {
		query = fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	}
	var rows []string
	if err := e.executor.Select(ctx, &rows, query); err != nil {
		return "", err
	}
	return strings.Join(rows, "\n"), nil
}

func (e *AthenaScrapper) getBasicSqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	db := e.executor.GetDb()
	query := scope.AppendScopeConditions(ctx, querySqlDefinitionsSQL, "t.table_catalog", "t.table_schema", "t.table_name")
	return stdsql.QueryMany(ctx, db, query,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Instance = e.executor.Instance()
			return row, nil
		}),
	)
}
