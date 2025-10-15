package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
        c.ordinal_position as "position"
	from 
		%[1]s.information_schema.columns as c
	join 
		%[1]s.information_schema.tables as t on 
		    c.table_catalog = t.table_catalog
			and c.table_name = t.table_name
			and c.table_schema = t.table_schema
	where UPPER(c.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
	`

func (e *SnowflakeScrapper) QueryCatalog(origCtx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	var finalResults []*scrapper.CatalogColumnRow
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
	g.SetLimit(4)

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
				rows, err := e.executor.GetDb().QueryxContext(groupCtx, fmt.Sprintf(catalogQuery, database))
				if err != nil {
					if isSharedDatabaseUnavailableError(err) {
						logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
							Warn("Shared database is no longer available, skipping")
						return nil
					}
					return errors.Wrapf(err, "failed to query catalog for database %s", database)
				}
				defer rows.Close()
				var tmpResults []*scrapper.CatalogColumnRow
				for rows.Next() {
					result := scrapper.CatalogColumnRow{}
					if err := rows.StructScan(&result); err != nil {
						return errors.Wrapf(err, "failed to scan catalog row for database %s", database)
					}
					result.Instance = e.conf.Account
					tmpResults = append(tmpResults, &result)
				}

				streamRows, err := e.showStreamsInDatabase(groupCtx, database)
				if err == nil {
					for _, streamRow := range streamRows {
						sourceTableParts := strings.Split(streamRow.TableName, ".")
						if len(sourceTableParts) != 3 {
							continue
						}

						var sourceTableColumns []*scrapper.CatalogColumnRow
						for _, result := range tmpResults {
							if result.Database == sourceTableParts[0] && result.Schema == sourceTableParts[1] && result.Table == sourceTableParts[2] {
								sourceTableColumns = append(sourceTableColumns, result)
							}
						}

						if len(sourceTableColumns) > 0 {
							for _, sourceTableColumn := range sourceTableColumns {
								tmpResults = append(
									tmpResults, &scrapper.CatalogColumnRow{
										Instance:       sourceTableColumn.Instance,
										Database:       streamRow.DatabaseName,
										Schema:         streamRow.SchemaName,
										Table:          streamRow.Name,
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
									},
								)
							}

							tmpResults = append(
								tmpResults, &scrapper.CatalogColumnRow{
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
							tmpResults = append(
								tmpResults, &scrapper.CatalogColumnRow{
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
							tmpResults = append(
								tmpResults, &scrapper.CatalogColumnRow{
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
						} else {
							logging.GetLogger(groupCtx).WithFields(
								logrus.Fields{
									"stream_table_name": streamRow.TableName,
									"database":          database,
								},
							).Warn("Failed to get columns of the STREAM")
						}
					}
				} else {
					logging.GetLogger(groupCtx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
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

	return finalResults, nil
}
