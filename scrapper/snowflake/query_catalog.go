package snowflake

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

var catalogQuery = `
	select
	    CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT() as "instance",
		c.table_catalog as "database",
        c.table_schema as "schema",
        c.table_name as "table",
        coalesce((t.table_type = 'MATERIALIZED VIEW' OR t.table_type = 'VIEW'), false) as "is_view",
        c.column_name as "column",
        c.data_type as "type",
        c.numeric_precision as "numeric_precision",
        c.numeric_scale as "numeric_scale",
        c.is_nullable = 'YES' as "is_nullable",
        c.comment as "comment",
        c.ordinal_position as "position"
	from
		%[1]s.information_schema.columns as c
	join
		%[1]s.information_schema.tables as t on
		    c.table_catalog = t.table_catalog
			and c.table_name = t.table_name
			and c.table_schema = t.table_schema
	where UPPER(c.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
		/* SYNQ_SCOPE_FILTER */
	`

func (e *SnowflakeScrapper) QueryCatalog(origCtx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	if e.conf.CatalogFromAccountUsage {
		return e.queryCatalogFromAccountUsage(origCtx)
	}
	databasesToQuery, err := e.GetDatabasesToQuery(origCtx)
	if err != nil {
		return nil, err
	}

	finalResults, err := e.queryCatalogIn(origCtx, databasesToQuery)
	if scrapper.Failed(err) {
		return nil, err
	}
	if tagErr := e.addTagReferences(origCtx, databasesToQuery, finalResults); tagErr != nil {
		return nil, tagErr
	}

	// Post-filter for SHOW STREAMS results which bypass SQL scope conditions.
	return scope.FilterRows(finalResults, scope.GetScope(origCtx)), err
}

// queryCatalogIn reads the columns of the tables and streams of the databases
// from their INFORMATION_SCHEMA and SHOW STREAMS, with no tags.
func (e *SnowflakeScrapper) queryCatalogIn(origCtx context.Context, databasesToQuery []string) ([]*scrapper.CatalogColumnRow, error) {
	return readDatabases(
		origCtx, databasesToQuery, 4, scrapper.OmittedColumns,
		func(groupCtx context.Context, database string) ([]*scrapper.CatalogColumnRow, error) {
			query := scope.AppendScopeConditions(groupCtx, fmt.Sprintf(catalogQuery, databaseIdent(database)), "", "c.table_schema", "c.table_name")
			rows, err := e.executor.QueryRows(groupCtx, query)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to query catalog for database %s", database)
			}
			defer rows.Close()
			tmpResults, err := scrapper.ScanAll[scrapper.CatalogColumnRow](
				groupCtx, rows, fmt.Sprintf("%s.information_schema.columns", database),
			)
			if err != nil {
				return nil, err
			}
			for _, result := range tmpResults {
				result.Instance = e.conf.Account
				result.ResolveNumeric(e.DialectType())
			}

			streamRows, err := e.showStreamsInDatabase(groupCtx, database)
			if err == nil {
				tmpResults = append(tmpResults, streamCatalogRows(groupCtx, streamRows, tmpResults)...)
			} else {
				logging.GetLogger(groupCtx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
			}

			return tmpResults, nil
		},
	)
}

// streamCatalogRows lists the columns of streams: the columns of the table each
// stream tracks, found in columns or among the streams listed before it, and
// the three METADATA$ columns a stream adds.
func streamCatalogRows(ctx context.Context, streamRows []*ShowStreamsRow, columns []*scrapper.CatalogColumnRow) []*scrapper.CatalogColumnRow {
	var out []*scrapper.CatalogColumnRow
	for _, streamRow := range streamRows {
		sourceTableParts := strings.Split(streamRow.TableName, ".")
		if len(sourceTableParts) != 3 {
			continue
		}

		var sourceTableColumns []*scrapper.CatalogColumnRow
		for _, result := range slices.Concat(columns, out) {
			if result.Database == sourceTableParts[0] && result.Schema == sourceTableParts[1] && result.Table == sourceTableParts[2] {
				sourceTableColumns = append(sourceTableColumns, result)
			}
		}

		if len(sourceTableColumns) == 0 {
			logging.GetLogger(ctx).WithFields(
				logrus.Fields{
					"stream_table_name": streamRow.TableName,
					"database":          streamRow.DatabaseName,
				},
			).Warn("Failed to get columns of the STREAM")
			continue
		}

		for _, sourceTableColumn := range sourceTableColumns {
			out = append(
				out, &scrapper.CatalogColumnRow{
					Instance:        sourceTableColumn.Instance,
					Database:        streamRow.DatabaseName,
					Schema:          streamRow.SchemaName,
					Table:           streamRow.Name,
					Column:          sourceTableColumn.Column,
					Type:            sourceTableColumn.Type,
					Position:        sourceTableColumn.Position,
					Comment:         sourceTableColumn.Comment,
					TableComment:    sourceTableColumn.TableComment,
					ColumnTags:      sourceTableColumn.ColumnTags,
					TableTags:       sourceTableColumn.TableTags,
					IsStructColumn:  sourceTableColumn.IsStructColumn,
					IsArrayColumn:   sourceTableColumn.IsArrayColumn,
					FieldSchemas:    sourceTableColumn.FieldSchemas,
					Precision:       sourceTableColumn.Precision,
					Scale:           sourceTableColumn.Scale,
					NumericCategory: sourceTableColumn.NumericCategory,
					IsNullable:      sourceTableColumn.IsNullable,
				},
			)
		}

		out = append(
			out, &scrapper.CatalogColumnRow{
				Instance: sourceTableColumns[0].Instance,
				Database: streamRow.DatabaseName,
				Schema:   streamRow.SchemaName,
				Table:    streamRow.Name,
				Column:   "METADATA$ACTION",
				Type:     "VARCHAR",
				Position: int32(len(sourceTableColumns) + 1),
				Comment:  lo.ToPtr("Specifies the action (INSERT or DELETE)."),
			},
		)
		out = append(
			out, &scrapper.CatalogColumnRow{
				Instance: sourceTableColumns[0].Instance,
				Database: streamRow.DatabaseName,
				Schema:   streamRow.SchemaName,
				Table:    streamRow.Name,
				Column:   "METADATA$ISUPDATE",
				Type:     "BOOLEAN",
				Position: int32(len(sourceTableColumns) + 2),
				Comment: lo.ToPtr(
					"Specifies whether the action recorded (INSERT or DELETE) is part of an UPDATE applied to the rows in the source table or view.",
				),
			},
		)
		out = append(
			out, &scrapper.CatalogColumnRow{
				Instance: sourceTableColumns[0].Instance,
				Database: streamRow.DatabaseName,
				Schema:   streamRow.SchemaName,
				Table:    streamRow.Name,
				Column:   "METADATA$ROW_ID",
				Type:     "ROWID",
				Position: int32(len(sourceTableColumns) + 3),
				Comment: lo.ToPtr(
					"Specifies the unique and immutable ID for the row, which can be used to track changes to specific rows over time.",
				),
			},
		)
	}
	return out
}
