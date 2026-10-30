package snowflake

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/DataDog/go-sqllexer"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/sqlparser"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

type hybridTableRow struct {
	TableCatalog string `db:"table_catalog"`
	TableSchema  string `db:"table_schema"`
	TableName    string `db:"table_name"`
}

type clusteringKeyRow struct {
	TableSchema   string `db:"table_schema"`
	TableName     string `db:"table_name"`
	ClusteringKey string `db:"clustering_key"`
}

func (e *SnowflakeScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var finalResults []*scrapper.TableConstraintRow
	var m sync.Mutex

	// SHOW ... KEYS and INFORMATION_SCHEMA list only what the role holds a privilege on.
	databasesToQuery, err := e.getDatabasesWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}

	// Check which of its optional columns information_schema.tables has, using
	// the first database, so an account without one still gets its other keys.
	clusteringKeyAvailable, isHybridAvailable := false, false
	if len(databasesToQuery) > 0 {
		columns, err := e.findTableColumns(ctx, fmt.Sprintf("%s.information_schema.tables", databasesToQuery[0]))
		if err != nil {
			logging.GetLogger(ctx).WithError(err).
				Debug("Failed to describe information_schema.tables, skipping clustering keys and hybrid tables")
		} else {
			clusteringKeyAvailable = containsFold(columns, "CLUSTERING_KEY")
			isHybridAvailable = containsFold(columns, "IS_HYBRID")
		}
		if !clusteringKeyAvailable {
			logging.GetLogger(ctx).Debug("CLUSTERING_KEY column not available in information_schema.tables, skipping clustering keys")
		}
		if !isHybridAvailable {
			logging.GetLogger(ctx).Debug("IS_HYBRID column not available in information_schema.tables, every key reads as not enforced")
		}
	}

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	for _, database := range databasesToQuery {
		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(func() error {
			var results []*scrapper.TableConstraintRow

			hybrid := hybridTables{}
			if isHybridAvailable {
				var err error
				hybrid, err = e.queryHybridTablesInDatabase(groupCtx, database)
				if err != nil {
					if isSharedDatabaseUnavailableError(err) {
						logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
							Warn("Shared database is no longer available, skipping")
						return nil
					}
					return errors.Wrapf(err, "failed to query hybrid tables for database %s", database)
				}
			}

			for _, keys := range []struct{ show, constraintType string }{
				{"PRIMARY KEYS", scrapper.ConstraintTypePrimaryKey},
				{"UNIQUE KEYS", scrapper.ConstraintTypeUniqueIndex},
			} {
				keyRows, err := e.queryKeysInDatabase(groupCtx, database, keys.show, keys.constraintType, hybrid)
				if err != nil {
					if isSharedDatabaseUnavailableError(err) {
						logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
							Warn("Shared database is no longer available, skipping")
						return nil
					}
					return errors.Wrapf(err, "failed to query %s for database %s", strings.ToLower(keys.show), database)
				}
				results = append(results, keyRows...)
			}

			fkRows, err := e.queryImportedKeysInDatabase(groupCtx, database, hybrid)
			if err != nil {
				if isSharedDatabaseUnavailableError(err) {
					logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
						Warn("Shared database is no longer available, skipping")
					return nil
				}
				return errors.Wrapf(err, "failed to query foreign keys for database %s", database)
			}
			results = append(results, fkRows...)

			if clusteringKeyAvailable {
				ckRows, err := e.queryClusteringKeysInDatabase(groupCtx, database)
				if err != nil {
					if isSharedDatabaseUnavailableError(err) {
						logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
							Warn("Shared database is no longer available, skipping")
						return nil
					}
					return errors.Wrapf(err, "failed to query clustering keys for database %s", database)
				}
				results = append(results, ckRows...)
			}

			m.Lock()
			defer m.Unlock()
			finalResults = append(finalResults, results...)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Post-filter results for the SHOW ... KEYS statements, which don't support WHERE clauses.
	return scope.FilterRows(finalResults, scope.GetScope(ctx)), nil
}

// hybridTables are the hybrid tables of a database. A hybrid table is the only
// Snowflake table that enforces its primary, unique and foreign keys: on any
// other table a key is recorded and never checked.
type hybridTables map[hybridTable]bool

type hybridTable struct{ database, schema, table string }

// enforces is whether Snowflake enforces the keys of the table.
func (h hybridTables) enforces(database, schema, table string) *bool {
	return lo.ToPtr(h[hybridTable{database, schema, table}])
}

// queryHybridTablesInDatabase reads which tables of a database are hybrid.
func (e *SnowflakeScrapper) queryHybridTablesInDatabase(ctx context.Context, database string) (hybridTables, error) {
	tableName := fmt.Sprintf("%s.information_schema.tables", database)
	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf(
		`SELECT table_catalog AS "table_catalog", table_schema AS "table_schema", table_name AS "table_name" FROM %s WHERE is_hybrid = 'YES'`,
		tableName,
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hybridRows, err := scrapper.ScanAll[hybridTableRow](ctx, rows, tableName)
	if err != nil {
		return nil, err
	}
	hybrid := hybridTables{}
	for _, row := range hybridRows {
		hybrid[hybridTable{row.TableCatalog, row.TableSchema, row.TableName}] = true
	}
	return hybrid, nil
}

// relyOf is the RELY of a key as SHOW ... KEYS lists it, nil when the output
// has no rely column.
func relyOf(row map[string]any) *bool {
	rely, ok := row["rely"]
	if !ok || fmt.Sprint(rely) == "" {
		return nil
	}
	return lo.ToPtr(strings.EqualFold(fmt.Sprint(rely), "true"))
}

// queryKeysInDatabase reads the primary or unique keys of a database's
// tables, SHOW PRIMARY KEYS or SHOW UNIQUE KEYS, one row per column.
func (e *SnowflakeScrapper) queryKeysInDatabase(
	ctx context.Context,
	database, show, constraintType string,
	hybrid hybridTables,
) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf("SHOW %s IN DATABASE %s", show, database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		tmp := map[string]any{}
		if err := rows.MapScan(tmp); err != nil {
			return nil, err
		}
		results = append(results, keyRow(e.conf.Account, constraintType, tmp, hybrid))
	}

	return results, rows.Err()
}

// keyRow is one column of a primary or unique key as SHOW PRIMARY KEYS or SHOW
// UNIQUE KEYS lists it.
func keyRow(account, constraintType string, row map[string]any, hybrid hybridTables) *scrapper.TableConstraintRow {
	keySequence, _ := strconv.ParseInt(fmt.Sprint(row["key_sequence"]), 10, 32)
	database, schema, table := fmt.Sprint(row["database_name"]), fmt.Sprint(row["schema_name"]), fmt.Sprint(row["table_name"])
	return &scrapper.TableConstraintRow{
		Instance:       account,
		Database:       database,
		Schema:         schema,
		Table:          table,
		ConstraintName: fmt.Sprint(row["constraint_name"]),
		ColumnName:     fmt.Sprint(row["column_name"]),
		ConstraintType: constraintType,
		ColumnPosition: int32(keySequence),
		IsEnforced:     hybrid.enforces(database, schema, table),
		IsRely:         relyOf(row),
	}
}

// queryImportedKeysInDatabase reads the foreign keys declared on the tables of
// a database, one row per column, each naming the column it references, which
// may be in another database.
func (e *SnowflakeScrapper) queryImportedKeysInDatabase(
	ctx context.Context,
	database string,
	hybrid hybridTables,
) ([]*scrapper.TableConstraintRow, error) {
	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf("SHOW IMPORTED KEYS IN DATABASE %s", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*scrapper.TableConstraintRow
	for rows.Next() {
		tmp := map[string]any{}
		if err := rows.MapScan(tmp); err != nil {
			return nil, err
		}
		results = append(results, importedKeyRow(e.conf.Account, tmp, hybrid))
	}
	return results, rows.Err()
}

// importedKeyRow is one column of a foreign key as SHOW IMPORTED KEYS lists it.
// The key is enforced when the table it is declared on is hybrid.
func importedKeyRow(account string, row map[string]any, hybrid hybridTables) *scrapper.TableConstraintRow {
	keySequence, _ := strconv.ParseInt(fmt.Sprint(row["key_sequence"]), 10, 32)
	database, schema, table := fmt.Sprint(row["fk_database_name"]), fmt.Sprint(row["fk_schema_name"]), fmt.Sprint(row["fk_table_name"])
	return &scrapper.TableConstraintRow{
		Instance:             account,
		Database:             database,
		Schema:               schema,
		Table:                table,
		ConstraintName:       fmt.Sprint(row["fk_name"]),
		ColumnName:           fmt.Sprint(row["fk_column_name"]),
		ConstraintType:       scrapper.ConstraintTypeForeignKey,
		ColumnPosition:       int32(keySequence),
		IsEnforced:           hybrid.enforces(database, schema, table),
		IsRely:               relyOf(row),
		ReferencedDatabase:   fmt.Sprint(row["pk_database_name"]),
		ReferencedSchema:     fmt.Sprint(row["pk_schema_name"]),
		ReferencedTable:      fmt.Sprint(row["pk_table_name"]),
		ReferencedColumnName: fmt.Sprint(row["pk_column_name"]),
	}
}

func containsFold(values []string, want string) bool {
	return slices.ContainsFunc(values, func(v string) bool { return strings.EqualFold(v, want) })
}

// queryClusteringKeysInDatabase reads the clustering key of each table of a
// database, one row per entry of the key, each carrying the whole key as
// Snowflake shows it (LINEAR(CATEGORY, ID)) as its expression.
func (e *SnowflakeScrapper) queryClusteringKeysInDatabase(ctx context.Context, database string) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	query := scope.AppendScopeConditions(
		ctx,
		fmt.Sprintf(
			`SELECT table_schema AS "table_schema", table_name AS "table_name", clustering_key AS "clustering_key" FROM %s.information_schema.tables WHERE clustering_key IS NOT NULL AND clustering_key != '' /* SYNQ_SCOPE_FILTER */`,
			database,
		),
		"", "table_schema", "table_name",
	)

	rows, err := e.executor.QueryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	clusteringRows, err := scrapper.ScanAll[clusteringKeyRow](ctx, rows, fmt.Sprintf("%s.information_schema.tables", database))
	if err != nil {
		return nil, err
	}
	for _, row := range clusteringRows {
		for i, entry := range clusteringKeyEntries(row.ClusteringKey) {
			results = append(results, &scrapper.TableConstraintRow{
				Instance:             e.conf.Account,
				Database:             database,
				Schema:               row.TableSchema,
				Table:                row.TableName,
				ConstraintName:       "clustering_key",
				ColumnName:           entry,
				ConstraintType:       scrapper.ConstraintTypeClusterBy,
				ColumnPosition:       int32(i + 1),
				ConstraintExpression: row.ClusteringKey,
			})
		}
	}

	return results, nil
}

// clusteringKeyEntries splits a clustering key as information_schema.tables
// shows it, LINEAR(CATEGORY, TO_DATE(CREATED_AT)), into its entries, in key
// order: a column by its name, unquoted, an expression as written. A comma
// inside an expression's parentheses or inside a quoted name does not split it.
func clusteringKeyEntries(key string) []string {
	tokens := sqlparser.ScanAllTokens(sqllexer.New(key, sqllexer.WithDBMS(sqllexer.DBMSSnowflake)))
	isPunctuation := func(tok *sqllexer.Token, value string) bool {
		return tok.Type == sqllexer.PUNCTUATION && tok.Value == value
	}

	// The entries are the arguments of LINEAR(...); a bare list is read as it is.
	significant := lo.Filter(tokens, func(tok *sqllexer.Token, _ int) bool { return tok.Type != sqllexer.SPACE })
	if len(significant) >= 3 && strings.EqualFold(significant[0].Value, "LINEAR") &&
		isPunctuation(significant[1], "(") && isPunctuation(significant[len(significant)-1], ")") {
		tokens = tokens[lo.IndexOf(tokens, significant[1])+1 : lo.IndexOf(tokens, significant[len(significant)-1])]
	}

	dialect := sqldialect.NewSnowflakeDialect()
	var entries []string
	var entry []*sqllexer.Token
	flush := func() {
		parts := lo.Filter(entry, func(tok *sqllexer.Token, _ int) bool { return tok.Type != sqllexer.SPACE })
		var text strings.Builder
		for _, tok := range entry {
			text.WriteString(tok.Value)
		}
		entry = nil
		if len(parts) == 0 {
			return
		}
		// The lexer reads a doubled quote inside a name as two quoted names.
		quotedName := lo.EveryBy(parts, func(tok *sqllexer.Token) bool { return tok.Type == sqllexer.QUOTED_IDENT })
		if name, quoted := dialect.UnquoteIdent(strings.TrimSpace(text.String())); quotedName && quoted {
			entries = append(entries, name)
			return
		}
		entries = append(entries, strings.TrimSpace(text.String()))
	}
	depth := 0
	for _, tok := range tokens {
		switch {
		case isPunctuation(tok, "("):
			depth++
		case isPunctuation(tok, ")"):
			depth--
		case depth == 0 && isPunctuation(tok, ","):
			flush()
			continue
		}
		entry = append(entry, tok)
	}
	flush()
	return entries
}
