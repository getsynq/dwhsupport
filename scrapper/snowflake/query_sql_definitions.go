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
	"github.com/getsynq/dwhsupport/sqlparser"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var sqlDefinitionsQuery = `
SELECT table_catalog as "database",
table_schema as "schema",
table_name as "table",
true as "is_view",
NVL2(view_definition,view_definition,'') as "sql"

FROM %[1]s.information_schema.views
where UPPER(table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
UNION ALL
SELECT table_catalog as "database",
table_schema as "schema",
table_name as "table",
false as "is_view",
'' as "sql"

FROM %[1]s.information_schema.tables
where UPPER(table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
AND table_type !='VIEW' 
AND table_type !='MATERIALIZED VIEW'
`

func (e *SnowflakeScrapper) QuerySqlDefinitions(origCtx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	var finalResults []*scrapper.SqlDefinitionRow
	var m sync.Mutex

	allDatabases, err := e.GetExistingDbs(origCtx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}

	g, groupCtx := errgroup.WithContext(origCtx)
	g.SetLimit(8)
	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			continue
		}

		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(
			func() error {

				var tmpResults []*scrapper.SqlDefinitionRow

				rows, err := e.executor.GetDb().QueryxContext(groupCtx, fmt.Sprintf(sqlDefinitionsQuery, database))
				if err != nil {
					return errors.Wrapf(err, "failed to query sql definitions for database %s", database)
				}
				defer rows.Close()

				for rows.Next() {
					result := scrapper.SqlDefinitionRow{}
					if err := rows.StructScan(&result); err != nil {
						return errors.Wrapf(err, "failed to scan sql definition row for database %s", database)
					}
					result.Instance = e.conf.Account
					tmpResults = append(tmpResults, &result)
				}

				streamRows, err := e.showStreamsInDatabase(groupCtx, database)
				if err == nil {
					for _, streamRow := range streamRows {

						tmpResults = append(
							tmpResults, &scrapper.SqlDefinitionRow{
								Instance:           e.conf.Account,
								Database:           streamRow.DatabaseName,
								Schema:             streamRow.SchemaName,
								Table:              streamRow.Name,
								IsView:             false,
								IsMaterializedView: false,
								Sql:                fmt.Sprintf("SELECT * FROM %s", streamRow.TableName),
							},
						)
					}
				} else {
					logging.GetLogger(origCtx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
				}

				m.Lock()
				defer m.Unlock()
				finalResults = append(finalResults, tmpResults...)

				return nil
			},
		)
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	if e.conf.NoGetDll {
		logging.GetLogger(origCtx).Info("skipping get ddl in sql definitions")
		return finalResults, nil
	}

	ignoreDbDdls := map[string]bool{}
	for _, db := range allDatabases {
		ignoreDbDdls[db.Name] = db.Kind == "IMPORTED DATABASE"
	}

	if len(finalResults) > 0 {
		perSchema := lo.GroupBy(
			finalResults, func(r *scrapper.SqlDefinitionRow) DatabaseAndSchema {
				return DatabaseAndSchema{DatabaseName: r.Database, SchemaName: r.Schema}
			},
		)

		g, groupCtx = errgroup.WithContext(origCtx)

		for databaseAndSchema, rows := range perSchema {
			rowsPerName := lo.Associate(
				rows, func(r *scrapper.SqlDefinitionRow) (string, *scrapper.SqlDefinitionRow) {
					return strings.ToUpper(r.Table), r
				},
			)

			if ignoreDbDdls[databaseAndSchema.DatabaseName] {
				continue
			}

			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			g.Go(
				func() error {
					ddls, err := e.getDdl(groupCtx, "SCHEMA", databaseAndSchema.DatabaseName, databaseAndSchema.SchemaName)
					if err != nil {
						logging.GetLogger(groupCtx).WithError(err).WithFields(
							logrus.Fields{
								"database": databaseAndSchema.DatabaseName,
								"schema":   databaseAndSchema.SchemaName,
							},
						).Error("failed to get ddl for schema")
						return nil
					}

					perFqn, err := ParseCreateStatementsPerObject(groupCtx, ddls)
					if err != nil {
						logging.GetLogger(groupCtx).WithError(err).WithFields(
							logrus.Fields{
								"database": databaseAndSchema.DatabaseName,
								"schema":   databaseAndSchema.SchemaName,
							},
						).Error("failed to parse ddl for schema")
					} else {
						for fqn, ddl := range perFqn {
							objectName := GetObjectNameFromFqn(fqn)

							if sqlDefRow, found := rowsPerName[strings.ToUpper(objectName)]; found {
								if sqlDefRow.Sql != "" {
									continue
								}
								sqlDefRow.Sql = ddl
							}
						}
					}
					return nil
				},
			)
		}

		err = g.Wait()
		if err != nil {
			return nil, err
		}

	}

	return finalResults, nil
}

func GetObjectNameFromFqn(fqn string) string {
	parts := strings.Split(fqn, ".")
	objectName := parts[len(parts)-1]
	return strings.ToUpper(UnQuote(objectName))
}

func ParseCreateStatementsPerObject(ctx context.Context, ddls string) (map[string]string, error) {

	logger := logging.GetLogger(ctx)

	lexer := sqllexer.New(ddls, sqllexer.WithDBMS(sqllexer.DBMSSnowflake))
	tokens := sqlparser.ScanAllTokens(lexer)
	statements := sqlparser.SplitTokensIntoStatements(tokens)

	res := map[string]string{}
	for _, statement := range statements {
		plainSql := sqlparser.PrintTokens(statement)
		if plainSql == "" || plainSql == ";" {
			continue
		}

		if created := parseCreatedFromStatement(logger, statement); created != nil {
			res[*created] = plainSql
		}

	}
	return res, nil
}

func parseCreatedFromStatement(logger logrus.FieldLogger, tokens []*sqllexer.Token) *string {
	parser := sqlparser.BaseParser{
		Tokens: tokens,
	}
	if !parser.ParseToken(sqllexer.Token{Type: sqllexer.COMMAND, Value: "CREATE"}) {
		return nil
	}

	parser.ParseToken(sqllexer.Token{Type: sqllexer.KEYWORD, Value: "OR"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.KEYWORD, Value: "REPLACE"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "TRANSIENT"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "TEMPORARY"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "DYNAMIC"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "HYBRID"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "MATERIALIZED"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.KEYWORD, Value: "RECURSIVE"})

	ind, nextToken := parser.PeekToken()
	switch strings.ToUpper(nextToken.Value) {
	case "SCHEMA", "PROCEDURE", "TASK", "STAGE", "FUNCTION", "TAG", "STREAMLIT":
		return nil
	case "TABLE", "VIEW", "STREAM":
		parser.Index = ind
		id, err := parser.ParseIdentifier()
		if err != nil {
			logger.WithError(err).Warnf("failed to parse identifier")
			return nil
		}
		return lo.ToPtr(id)

	default:
		logger.Warnf("expected supported object kind but got %s", sqlparser.DumpToken(nextToken))
		return nil
	}
}

type DatabaseAndSchema struct {
	DatabaseName string
	SchemaName   string
}

func UnQuote(key string) string {
	if str, err := strconv.Unquote(key); err == nil {
		key = str
	}
	if strings.HasPrefix(key, "'") {
		key = strings.Trim(key, "'")
	}
	return key
}

func (e *SnowflakeScrapper) getDdl(ctx context.Context, kind string, parts ...string) (string, error) {
	var res []string
	var err = e.executor.GetDb().SelectContext(ctx, &res, fmt.Sprintf("SELECT GET_DDL('%s', '%s', TRUE)", kind, strings.Join(parts, ".")))
	if len(res) > 0 {
		return fixDdl(res[0]), nil
	}
	return "", err
}

var ddlReplacer = strings.NewReplacer(
	"#UNKNOWN_POLICY", "UNKNOWN_POLICY",
	"#unknown_policy", "unknown_policy",
	"#UNKNOWN_TAG", "UNKNOWN_TAG",
	"#unknown_tag", "unknown_tag",
)

func fixDdl(s string) string {
	return ddlReplacer.Replace(s)
}
