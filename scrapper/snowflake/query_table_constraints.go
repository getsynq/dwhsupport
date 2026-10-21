package snowflake

import (
	"context"
	"fmt"
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

type clusteringKeyRow struct {
	TableSchema   string `db:"table_schema"`
	TableName     string `db:"table_name"`
	ClusteringKey string `db:"clustering_key"`
}

func (e *SnowflakeScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var finalResults []*scrapper.TableConstraintRow
	var m sync.Mutex

	databasesToQuery, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	// Check that information_schema.tables has the CLUSTERING_KEY column, using
	// the first database, so an account without it still gets its other keys.
	clusteringKeyAvailable := false
	if len(databasesToQuery) > 0 {
		available, err := e.hasClusteringKeyColumn(ctx, databasesToQuery[0])
		if err != nil {
			logging.GetLogger(ctx).WithError(err).Debug("Failed to check CLUSTERING_KEY column availability, skipping clustering keys")
		} else {
			clusteringKeyAvailable = available
		}
		if !clusteringKeyAvailable {
			logging.GetLogger(ctx).Debug("CLUSTERING_KEY column not available in information_schema.tables, skipping clustering keys")
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

			for _, keys := range []struct{ show, constraintType string }{
				{"PRIMARY KEYS", scrapper.ConstraintTypePrimaryKey},
				{"UNIQUE KEYS", scrapper.ConstraintTypeUniqueIndex},
			} {
				keyRows, err := e.queryKeysInDatabase(groupCtx, database, keys.show, keys.constraintType)
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

			fkRows, err := e.queryImportedKeysInDatabase(groupCtx, database)
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

// queryKeysInDatabase reads the primary or unique keys of a database's
// tables, SHOW PRIMARY KEYS or SHOW UNIQUE KEYS, one row per column. RELY is
// reported as whether the key is enforced: Snowflake enforces neither, and RELY
// says the optimizer may take it as true.
func (e *SnowflakeScrapper) queryKeysInDatabase(
	ctx context.Context,
	database, show, constraintType string,
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
		keySequence, _ := strconv.ParseInt(fmt.Sprint(tmp["key_sequence"]), 10, 32)
		var isEnforced *bool
		if rely, ok := tmp["rely"]; ok && fmt.Sprint(rely) != "" {
			v := strings.EqualFold(fmt.Sprint(rely), "true")
			isEnforced = &v
		}
		results = append(results, &scrapper.TableConstraintRow{
			Instance:       e.conf.Account,
			Database:       fmt.Sprint(tmp["database_name"]),
			Schema:         fmt.Sprint(tmp["schema_name"]),
			Table:          fmt.Sprint(tmp["table_name"]),
			ConstraintName: fmt.Sprint(tmp["constraint_name"]),
			ColumnName:     fmt.Sprint(tmp["column_name"]),
			ConstraintType: constraintType,
			ColumnPosition: int32(keySequence),
			IsEnforced:     isEnforced,
		})
	}

	return results, rows.Err()
}

// queryImportedKeysInDatabase reads the foreign keys declared on the tables of
// a database, one row per column, each naming the column it references, which
// may be in another database.
func (e *SnowflakeScrapper) queryImportedKeysInDatabase(ctx context.Context, database string) ([]*scrapper.TableConstraintRow, error) {
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
		results = append(results, importedKeyRow(e.conf.Account, tmp))
	}
	return results, rows.Err()
}

// importedKeyRow is one column of a foreign key as SHOW IMPORTED KEYS lists it.
// RELY is reported as whether the key is enforced, as for a primary key.
func importedKeyRow(account string, row map[string]any) *scrapper.TableConstraintRow {
	keySequence, _ := strconv.ParseInt(fmt.Sprint(row["key_sequence"]), 10, 32)
	var isEnforced *bool
	if rely, ok := row["rely"]; ok && fmt.Sprint(rely) != "" {
		v := strings.EqualFold(fmt.Sprint(rely), "true")
		isEnforced = &v
	}
	return &scrapper.TableConstraintRow{
		Instance:             account,
		Database:             fmt.Sprint(row["fk_database_name"]),
		Schema:               fmt.Sprint(row["fk_schema_name"]),
		Table:                fmt.Sprint(row["fk_table_name"]),
		ConstraintName:       fmt.Sprint(row["fk_name"]),
		ColumnName:           fmt.Sprint(row["fk_column_name"]),
		ConstraintType:       scrapper.ConstraintTypeForeignKey,
		ColumnPosition:       int32(keySequence),
		IsEnforced:           isEnforced,
		ReferencedDatabase:   fmt.Sprint(row["pk_database_name"]),
		ReferencedSchema:     fmt.Sprint(row["pk_schema_name"]),
		ReferencedTable:      fmt.Sprint(row["pk_table_name"]),
		ReferencedColumnName: fmt.Sprint(row["pk_column_name"]),
	}
}

// hasClusteringKeyColumn checks whether information_schema.tables has the
// CLUSTERING_KEY column by describing the table, so an account without it does
// not fail the whole read.
func (e *SnowflakeScrapper) hasClusteringKeyColumn(ctx context.Context, database string) (bool, error) {
	tableName := fmt.Sprintf("%s.information_schema.tables", database)
	columns, err := e.findTableColumns(ctx, tableName)
	if err != nil {
		return false, err
	}
	for _, col := range columns {
		if strings.EqualFold(col, "CLUSTERING_KEY") {
			return true, nil
		}
	}
	return false, nil
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
