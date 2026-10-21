package snowflake

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/DataDog/go-sqllexer"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
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
/* SYNQ_SCOPE_FILTER */
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
/* SYNQ_SCOPE_FILTER */
`

func (e *SnowflakeScrapper) QuerySqlDefinitions(origCtx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	databasesToQuery, err := e.GetDatabasesToQuery(origCtx)
	if err != nil {
		return nil, err
	}

	// incomplete names the databases left out, and goes back with the rows.
	finalResults, incomplete := readDatabases(
		origCtx, databasesToQuery, 8, scrapper.OmittedDefinitions,
		func(groupCtx context.Context, database string) ([]*scrapper.SqlDefinitionRow, error) {
			var tmpResults []*scrapper.SqlDefinitionRow

			query := scope.AppendScopeConditions(groupCtx, fmt.Sprintf(sqlDefinitionsQuery, database), "", "table_schema", "table_name")
			rows, err := e.executor.QueryRows(groupCtx, query)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to query sql definitions for database %s", database)
			}
			defer rows.Close()

			definitionRows, err := scrapper.ScanAll[scrapper.SqlDefinitionRow](
				groupCtx, rows, fmt.Sprintf("%s.information_schema.views", database),
			)
			if err != nil {
				return nil, err
			}
			for _, result := range definitionRows {
				result.Instance = e.conf.Account
				tmpResults = append(tmpResults, result)
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
				logging.GetLogger(groupCtx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
			}

			return tmpResults, nil
		},
	)
	if scrapper.Failed(incomplete) {
		return nil, incomplete
	}

	// Post-filter for SHOW STREAMS results which bypass SQL scope conditions.
	finalResults = scope.FilterRows(finalResults, scope.GetScope(origCtx))

	if e.conf.NoGetDll {
		logging.GetLogger(origCtx).Info("skipping get ddl in sql definitions")
		return finalResults, incomplete
	}

	allDatabases, err := e.GetExistingDbs(origCtx)
	if err != nil {
		return nil, err
	}
	ignoreDbDdls := sharedDatabases(allDatabases)

	if len(finalResults) > 0 {
		perSchema := lo.GroupBy(
			finalResults, func(r *scrapper.SqlDefinitionRow) DatabaseAndSchema {
				return DatabaseAndSchema{DatabaseName: r.Database, SchemaName: r.Schema}
			},
		)

		g, groupCtx := errgroup.WithContext(origCtx)
		g.SetLimit(8)

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
								// A view already has its SQL from information_schema.views,
								// but its tags are only here.
								tags := ParseDdlTags(ddl)
								sqlDefRow.Tags = tags.Table
								sqlDefRow.ColumnTags = tags.Columns
								if sqlDefRow.Sql != "" {
									continue
								}
								sqlDefRow.Sql = ddl
								sqlDefRow.Description = ParseCommentClause(ddl)
							}
						}
					}
					return nil
				},
			)
		}

		err := g.Wait()
		if err != nil {
			return nil, err
		}

	}

	return finalResults, incomplete
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
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "SECURE"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.IDENT, Value: "MATERIALIZED"})
	parser.ParseToken(sqllexer.Token{Type: sqllexer.KEYWORD, Value: "RECURSIVE"})

	ind, nextToken := parser.PeekToken()
	switch strings.ToUpper(nextToken.Value) {
	case "SCHEMA", "PROCEDURE", "TASK", "STAGE", "FUNCTION", "TAG", "STREAMLIT", "PIPE":
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

// sharedDatabases names the databases another account or Snowflake itself
// provides: an imported share, and an application such as the SNOWFLAKE
// database. GET_DDL refuses every object in one ("not supported on shared
// database"), so asking only repeats the same failure on every scan. They are
// matched by kind rather than by origin: a replica can carry an origin and
// still answers GET_DDL.
func sharedDatabases(databases []*DbDesc) map[string]bool {
	shared := map[string]bool{}
	for _, database := range databases {
		if database.Kind == "IMPORTED DATABASE" || database.Kind == "APPLICATION" {
			shared[database.Name] = true
		}
	}
	return shared
}

func (e *SnowflakeScrapper) getDdl(ctx context.Context, kind string, parts ...string) (string, error) {
	query, err := getDdlQuery(kind, parts...)
	if err != nil {
		return "", err
	}
	var res []string
	err = e.executor.Select(ctx, &res, query)
	if len(res) > 0 {
		return fixDdl(res[0]), nil
	}
	return "", err
}

// getDdlQuery builds the GET_DDL statement for the object the parts name, from
// the database down. Each part is a name as Snowflake itself reports it, out
// of information_schema, and is quoted, because only a quoted name is resolved
// exactly as written. Unquoted, a lower-case schema `raw` folds to `RAW`:
// GET_DDL then answers "does not exist or not authorized" when there is no
// `RAW`, and returns the other schema's DDL when there is one.
func getDdlQuery(kind string, parts ...string) (string, error) {
	dialect := sqldialect.NewSnowflakeDialect()
	idents := lo.Map(parts, func(part string, _ int) sqldialect.Ident { return sqldialect.CanonicalIdent(part) })
	name, err := sqldialect.QualifiedIdent(idents...).ToSql(dialect)
	if err != nil {
		return "", errors.Wrapf(err, "failed to quote the name of the %s to read the DDL of", kind)
	}
	return fmt.Sprintf("SELECT GET_DDL(%s, %s, TRUE)", dialect.StringLiteral(kind), dialect.StringLiteral(name)), nil
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

// ParseWithTagClause extracts the object's own tags from a Snowflake DDL's
// WITH TAG (...) clause, leaving out the tags of its columns.
// It skips UNKNOWN_TAG sentinel entries that indicate insufficient permissions.
func ParseWithTagClause(ddl string) []*scrapper.Tag {
	return ParseDdlTags(ddl).Table
}

// DdlTags are the tags one CREATE statement assigns.
type DdlTags struct {
	// Table holds the object's own tags.
	Table []*scrapper.Tag
	// Columns holds each column's tags by the column's name as the DDL writes
	// it, unquoted. Nil when no column has a tag.
	Columns map[string][]*scrapper.Tag
}

// ParseDdlTags reads every WITH TAG (...) clause of a CREATE TABLE or CREATE
// VIEW statement as GET_DDL writes it. A column's tags sit inside the column
// list, after the column's name and type, and the object's own tags follow the
// column list. A view's query after its top-level AS is never read, so a CTE in
// it cannot pass for a tag.
// It skips UNKNOWN_TAG sentinel entries that indicate insufficient permissions.
func ParseDdlTags(ddl string) DdlTags {
	lexer := sqllexer.New(ddl, sqllexer.WithDBMS(sqllexer.DBMSSnowflake))
	tokens := sqlparser.ScanAllTokens(lexer)
	parser := sqlparser.BaseParser{Tokens: tokens}

	var result DdlTags
	depth := 0
	// The column list is the first top-level parenthesis, except the key of a
	// CLUSTER BY written before it.
	seenColumnList, inColumnList := false, false
	atColumnStart := false
	column := ""
	var previous *sqllexer.Token

	for {
		ind, tok := parser.PeekToken()
		if tok.Type == sqllexer.EOF {
			return result
		}
		parser.Index = ind
		last := previous
		previous = tok

		switch {
		case tok.Type == sqllexer.PUNCTUATION && tok.Value == "(":
			depth++
			if depth == 1 && !seenColumnList && (last == nil || strings.ToUpper(last.Value) != "BY") {
				seenColumnList, inColumnList, atColumnStart = true, true, true
			}
			continue
		case tok.Type == sqllexer.PUNCTUATION && tok.Value == ")":
			depth--
			if depth == 0 {
				inColumnList = false
			}
			continue
		case inColumnList && depth == 1 && tok.Type == sqllexer.PUNCTUATION && tok.Value == ",":
			atColumnStart, column = true, ""
			continue
		case inColumnList && depth == 1 && atColumnStart:
			atColumnStart = false
			// A constraint is listed with the columns and names none.
			if sqlparser.IsIdentifierLikeToken(tok) && !(tok.Type != sqllexer.QUOTED_IDENT && strings.EqualFold(tok.Value, "CONSTRAINT")) {
				column = strings.Join(unquoteNameParts(tok.Value), ".")
			}
			continue
		case depth == 0 && tok.Type != sqllexer.QUOTED_IDENT && strings.EqualFold(tok.Value, "AS"):
			return result
		}

		if !strings.EqualFold(tok.Value, "WITH") || tok.Type == sqllexer.QUOTED_IDENT {
			continue
		}
		owner := ""
		switch {
		case depth == 0:
		case inColumnList && depth == 1 && column != "":
			owner = column
		default:
			continue
		}
		afterWith := parser.Index
		tagInd, tagTok := parser.PeekToken()
		if !strings.EqualFold(tagTok.Value, "TAG") {
			continue
		}
		parser.Index = tagInd
		openInd, openTok := parser.PeekToken()
		if openTok.Type != sqllexer.PUNCTUATION || openTok.Value != "(" {
			parser.Index = afterWith
			continue
		}
		parser.Index = openInd
		previous = openTok
		// The closing parenthesis is left to the loop, which takes depth back.
		depth++
		tags := parseTagAssignments(&parser)
		if owner == "" {
			result.Table = append(result.Table, tags...)
		} else if len(tags) > 0 {
			if result.Columns == nil {
				result.Columns = map[string][]*scrapper.Tag{}
			}
			result.Columns[owner] = append(result.Columns[owner], tags...)
		}
	}
}

// parseTagAssignments parses tag_name='value' pairs inside WITH TAG (...).
// Parser must be positioned right after the opening '('.
func parseTagAssignments(parser *sqlparser.BaseParser) []*scrapper.Tag {
	var tags []*scrapper.Tag

	for {
		// Parse tag name (fully qualified identifier like DB.SCHEMA.TAG_NAME)
		tagName, ok := parseTagName(parser)
		if !ok {
			break
		}

		// Expect '='
		ind, tok := parser.PeekToken()
		if tok.Type != sqllexer.OPERATOR || tok.Value != "=" {
			break
		}
		parser.Index = ind

		// Expect quoted value
		ind, tok = parser.PeekToken()
		if tok.Type != sqllexer.STRING {
			break
		}
		parser.Index = ind
		tagValue := strings.Trim(tok.Value, "'")

		// Skip UNKNOWN_TAG sentinel values (insufficient permissions)
		if !isUnknownTag(tagName, tagValue) {
			tags = append(tags, &scrapper.Tag{
				TagName:  tagName,
				TagValue: tagValue,
				Kind:     scrapper.TagKindGovernedTag,
			})
		}

		// Expect ',' or ')'
		ind, tok = parser.PeekToken()
		if tok.Type == sqllexer.PUNCTUATION && tok.Value == "," {
			parser.Index = ind
			continue
		}
		// ')' or anything else means we're done
		break
	}

	return tags
}

// parseTagName parses a potentially dot-qualified tag name like DB.SCHEMA.TAG_NAME.
func parseTagName(parser *sqlparser.BaseParser) (string, bool) {
	var parts []string

	for {
		ind, tok := parser.PeekToken()
		if !sqlparser.IsIdentifierLikeToken(tok) {
			return "", false
		}
		parser.Index = ind
		// The lexer keeps a dotted name in one token, and a tag created with a
		// quoted name is written quoted while Snowflake stores it without them.
		parts = append(parts, unquoteNameParts(tok.Value)...)

		// Check for dot separator
		ind, tok = parser.PeekToken()
		if tok.Type == sqllexer.PUNCTUATION && tok.Value == "." {
			parser.Index = ind
			continue
		}
		// Not a dot — done with this tag name
		break
	}

	return strings.Join(parts, "."), len(parts) > 0
}

// unquoteNameParts splits a possibly dot-qualified name as a DDL writes it into
// its parts, each without its quotes. An unquoted part is already the name as
// Snowflake stores it, since GET_DDL quotes every name that would fold.
func unquoteNameParts(text string) []string {
	dialect := sqldialect.NewSnowflakeDialect()
	return lo.Map(sqldialect.SplitQualifiedIdent(text), func(part string, _ int) string {
		if name, quoted := dialect.UnquoteIdent(part); quoted {
			return name
		}
		return part
	})
}

// ParseCommentClause extracts the table-level COMMENT='...' value from a Snowflake DDL statement.
// It only considers COMMENT tokens that appear after the outermost closing parenthesis
// of the column definitions, to avoid matching column-level comments.
func ParseCommentClause(ddl string) *string {
	lexer := sqllexer.New(ddl, sqllexer.WithDBMS(sqllexer.DBMSSnowflake))
	tokens := sqlparser.ScanAllTokens(lexer)
	parser := sqlparser.BaseParser{Tokens: tokens}

	// Track parenthesis depth to skip column definitions
	depth := 0
	for {
		ind, tok := parser.PeekToken()
		if tok.Type == sqllexer.EOF {
			return nil
		}
		parser.Index = ind

		if tok.Type == sqllexer.PUNCTUATION && tok.Value == "(" {
			depth++
			continue
		}
		if tok.Type == sqllexer.PUNCTUATION && tok.Value == ")" {
			depth--
			continue
		}

		// Only match COMMENT outside of parentheses
		if depth > 0 {
			continue
		}

		if strings.ToUpper(tok.Value) != "COMMENT" {
			continue
		}

		// Accept both COMMENT='value' and COMMENT 'value'
		nextInd, nextTok := parser.PeekToken()
		if nextTok.Type == sqllexer.OPERATOR && nextTok.Value == "=" {
			parser.Index = nextInd
			nextInd, nextTok = parser.PeekToken()
		}

		if nextTok.Type != sqllexer.STRING {
			continue
		}
		parser.Index = nextInd
		comment := strings.Trim(nextTok.Value, "'")
		if comment == "" {
			return nil
		}
		return &comment
	}
}

func isUnknownTag(tagName, tagValue string) bool {
	upperName := strings.ToUpper(tagName)
	upperValue := strings.ToUpper(tagValue)
	return upperName == "UNKNOWN_TAG" || strings.HasSuffix(upperName, ".UNKNOWN_TAG") ||
		upperValue == "UNKNOWN_VALUE" || upperValue == "#UNKNOWN_VALUE"
}
