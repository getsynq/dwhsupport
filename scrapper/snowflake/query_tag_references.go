package snowflake

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

// tagReferencesQuery lists the tags assigned directly to tables, views and
// their columns. ACCOUNT_USAGE has no masking, unlike GET_DDL without APPLY
// TAG, and lags behind by up to two hours. A tag inherited from a database or
// a schema is left out, as it is no assignment of the object's own.
var tagReferencesQuery = `
SELECT object_database AS "database",
       object_schema AS "schema",
       object_name AS "table",
       NVL(column_name, '') AS "column",
       domain AS "domain",
       tag_database AS "tag_database",
       tag_schema AS "tag_schema",
       tag_name AS "tag_name",
       NVL(tag_value, '') AS "tag_value"
FROM %[1]s.ACCOUNT_USAGE.TAG_REFERENCES
WHERE object_deleted IS NULL
  AND domain IN ('TABLE', 'COLUMN')
  AND UPPER(object_database) IN (%[2]s)
/* SYNQ_SCOPE_FILTER */
ORDER BY tag_database, tag_schema, tag_name, tag_value
`

type tagReferenceRow struct {
	Database    string `db:"database"`
	Schema      string `db:"schema"`
	Table       string `db:"table"`
	Column      string `db:"column"`
	Domain      string `db:"domain"`
	TagDatabase string `db:"tag_database"`
	TagSchema   string `db:"tag_schema"`
	TagName     string `db:"tag_name"`
	TagValue    string `db:"tag_value"`
}

type tagReferenceTable struct {
	database, schema, table string
}

// tagReferences are the tags of the tables and columns of one scan, by the
// names Snowflake reports in information_schema.
type tagReferences struct {
	tables  map[tagReferenceTable][]*scrapper.Tag
	columns map[tagReferenceTable]map[string][]*scrapper.Tag
}

// queryTagReferences reads the tags of every object in the databases. It
// answers nil and an error when TAG_REFERENCES cannot be read; the caller goes
// on without these tags.
func (e *SnowflakeScrapper) queryTagReferences(ctx context.Context, databases []string) (*tagReferences, error) {
	result := &tagReferences{
		tables:  map[tagReferenceTable][]*scrapper.Tag{},
		columns: map[tagReferenceTable]map[string][]*scrapper.Tag{},
	}
	if len(databases) == 0 {
		return result, nil
	}
	dialect := sqldialect.NewSnowflakeDialect()
	names := lo.Map(databases, func(database string, _ int) string { return dialect.StringLiteral(strings.ToUpper(database)) })
	query := scope.AppendScopeConditions(
		ctx,
		fmt.Sprintf(tagReferencesQuery, e.accountUsageDb(), strings.Join(names, ", ")),
		"object_database", "object_schema", "object_name",
	)
	rows, err := e.executor.QueryRows(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query tag references")
	}
	defer rows.Close()
	references, err := scrapper.ScanAll[tagReferenceRow](ctx, rows, "ACCOUNT_USAGE.TAG_REFERENCES")
	if err != nil {
		return nil, errors.Wrap(err, "failed to read tag references")
	}

	seen := map[string]bool{}
	for _, reference := range references {
		key := tagReferenceTable{database: reference.Database, schema: reference.Schema, table: reference.Table}
		tag := &scrapper.Tag{
			TagName:  strings.Join([]string{reference.TagDatabase, reference.TagSchema, reference.TagName}, "."),
			TagValue: reference.TagValue,
			Kind:     scrapper.TagKindGovernedTag,
		}
		dedupKey := strings.Join([]string{key.database, key.schema, key.table, reference.Column, tag.TagName, tag.TagValue}, "\x00")
		if seen[dedupKey] {
			continue
		}
		seen[dedupKey] = true
		switch reference.Domain {
		case "TABLE":
			result.tables[key] = append(result.tables[key], tag)
		case "COLUMN":
			if reference.Column == "" {
				continue
			}
			if result.columns[key] == nil {
				result.columns[key] = map[string][]*scrapper.Tag{}
			}
			result.columns[key][reference.Column] = append(result.columns[key][reference.Column], tag)
		}
	}
	return result, nil
}

// apply sets the tags on the catalog rows of the objects they belong to.
func (r *tagReferences) apply(rows []*scrapper.CatalogColumnRow) {
	for _, row := range rows {
		key := tagReferenceTable{database: row.Database, schema: row.Schema, table: row.Table}
		// Each row gets its own slice: the wrappers filter tags in place.
		if tags := r.tables[key]; len(tags) > 0 {
			row.TableTags = slices.Clone(tags)
		}
		if tags := r.columns[key][row.Column]; len(tags) > 0 {
			row.ColumnTags = slices.Clone(tags)
		}
	}
}

// addTagReferences reads TAG_REFERENCES and sets the tags it lists on the rows.
// The read is best-effort: a role without IMPORTED PRIVILEGES on the SNOWFLAKE
// database, or an AccountUsageDb that does not exist, leaves the rows without
// these tags, and their tables keep what GET_DDL gives in QuerySqlDefinitions.
func (e *SnowflakeScrapper) addTagReferences(ctx context.Context, databases []string, rows []*scrapper.CatalogColumnRow) error {
	references, err := e.queryTagReferences(ctx, databases)
	return e.applyTagReferences(ctx, references, err, rows)
}

// applyTagReferences sets the tags a TAG_REFERENCES read gave on the rows, and
// logs a read that failed, as addTagReferences describes.
func (e *SnowflakeScrapper) applyTagReferences(
	ctx context.Context,
	references *tagReferences,
	err error,
	rows []*scrapper.CatalogColumnRow,
) error {
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		logger := logging.GetLogger(ctx).WithError(err).WithFields(logrus.Fields{"account_usage_db": e.accountUsageDb()})
		if isRefused(err) {
			logger.Warn("tag references refused, catalog read without them")
		} else {
			logger.Error("failed to read tag references, catalog read without them")
		}
		return nil
	}
	references.apply(rows)
	return nil
}
