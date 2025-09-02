package snowflake

import (
	"context"
	"fmt"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

var catalogQuery = `
	select 
	    CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT() as "instance",
		c.table_catalog as "database",
        c.table_schema as "schema",
        c.table_name as "table",
        (t.table_type = 'MATERIALIZED VIEW' OR t.table_type = 'VIEW') as "is_view",
        c.column_name as "column",
        c.data_type as "type",
        c.ordinal_position as "position"
	from 
		%[1]s.information_schema.columns as c
	left join 
		%[1]s.information_schema.tables as t on 
		    c.table_catalog = t.table_catalog
			and c.table_name = t.table_name
			and c.table_schema = t.table_schema
	where UPPER(c.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
	`

func (e *SnowflakeScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	var results []*scrapper.CatalogColumnRow

	allDatabases, err := e.allAllowedDatabases(ctx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}
	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			continue
		}

		rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(catalogQuery, database))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			result := scrapper.CatalogColumnRow{}
			if err := rows.StructScan(&result); err != nil {
				return nil, err
			}
			result.Instance = e.conf.Account
			results = append(results, &result)
		}

	}

	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			continue
		}

		streamRows, err := e.showStreamsInDatabase(ctx, database)
		if err == nil {
			for _, streamRow := range streamRows {
				sourceTableParts := strings.Split(streamRow.TableName, ".")
				if len(sourceTableParts) != 3 {
					continue
				}

				var sourceTableColumns []*scrapper.CatalogColumnRow
				for _, result := range results {
					if result.Database == sourceTableParts[0] && result.Schema == sourceTableParts[1] && result.Table == sourceTableParts[2] {
						sourceTableColumns = append(sourceTableColumns, result)
					}
				}

				if len(sourceTableColumns) > 0 {
					for _, sourceTableColumn := range sourceTableColumns {
						results = append(results, &scrapper.CatalogColumnRow{
							Instance:       sourceTableColumn.Instance,
							Database:       streamRow.DatabaseName,
							Schema:         streamRow.SchemaName,
							Table:          streamRow.TableName,
							Column:         sourceTableColumn.Column,
							Type:           sourceTableColumn.Type,
							Position:       sourceTableColumn.Position,
							Comment:        sourceTableColumn.Comment,
							TableComment:   sourceTableColumn.TableComment,
							ColumnTags:     sourceTableColumn.ColumnTags,
							TableTags:      sourceTableColumn.TableTags,
							IsStructColumn: sourceTableColumn.IsStructColumn,
							IsArrayColumn:  sourceTableColumn.IsArrayColumn,
							FieldSchemas:   sourceTableColumn.FieldSchemas,
						})
					}

					results = append(results, &scrapper.CatalogColumnRow{
						Instance: sourceTableColumns[0].Instance,
						Database: streamRow.DatabaseName,
						Schema:   streamRow.SchemaName,
						Table:    streamRow.TableName,
						Column:   "METADATA$ACTION",
						Type:     "VARCHAR",
						Position: int32(len(sourceTableColumns) + 1),
						Comment:  lo.ToPtr("Specifies the action (INSERT or DELETE)."),
					})
					results = append(results, &scrapper.CatalogColumnRow{
						Instance: sourceTableColumns[0].Instance,
						Database: streamRow.DatabaseName,
						Schema:   streamRow.SchemaName,
						Table:    streamRow.TableName,
						Column:   "METADATA$ISUPDATE",
						Type:     "BOOLEAN",
						Position: int32(len(sourceTableColumns) + 2),
						Comment:  lo.ToPtr("Specifies whether the action recorded (INSERT or DELETE) is part of an UPDATE applied to the rows in the source table or view."),
					})
					results = append(results, &scrapper.CatalogColumnRow{
						Instance: sourceTableColumns[0].Instance,
						Database: streamRow.DatabaseName,
						Schema:   streamRow.SchemaName,
						Table:    streamRow.TableName,
						Column:   "METADATA$ROW_ID",
						Type:     "ROWID",
						Position: int32(len(sourceTableColumns) + 3),
						Comment:  lo.ToPtr("Specifies the unique and immutable ID for the row, which can be used to track changes to specific rows over time."),
					})
				} else {
					logging.GetLogger(ctx).WithFields(logrus.Fields{
						"stream_table_name": streamRow.TableName,
					}).Warn("Failed to get columns of the STREAM")
				}
			}
		} else {
			logging.GetLogger(ctx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
		}

	}

	return results, nil
}
