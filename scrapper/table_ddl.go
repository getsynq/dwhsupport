package scrapper

import (
	"context"

	"github.com/getsynq/dwhsupport/logging"
	"golang.org/x/sync/errgroup"
)

// ReadTableDdls fills each table's Sql with the DDL that read returns for it,
// running at most parallel reads at a time, for a warehouse that answers a
// table's DDL only one statement per table (Redshift SHOW TABLE, Oracle
// DBMS_METADATA.GET_DDL).
//
// It returns the tables that got a DDL, in the order they were given. A table
// whose read fails is left out:
//   - quietly, when gone reports the error as the table having been dropped
//     since it was listed;
//   - as an OmittedDefinitions omission, when the caller accepts incomplete
//     results (WithIncompleteResults);
//   - with a warning otherwise, as the table never had a definition before
//     these reads existed.
//
// Only a done context fails the whole read.
func ReadTableDdls(
	ctx context.Context,
	tables []*SqlDefinitionRow,
	parallel int,
	read func(ctx context.Context, table *SqlDefinitionRow) (string, error),
	gone func(err error) bool,
) ([]*SqlDefinitionRow, []Omission, error) {
	log := logging.GetLogger(ctx)
	acceptIncomplete := IncompleteResultsAccepted(ctx)

	ddls := make([]string, len(tables))
	failures := make([]error, len(tables))

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	for i, table := range tables {
		if groupCtx.Err() != nil {
			break
		}
		g.Go(func() error {
			ddl, err := read(groupCtx, table)
			if err == nil {
				ddls[i] = ddl
				return nil
			}
			if groupCtx.Err() != nil {
				return groupCtx.Err()
			}
			if gone(err) {
				log.WithError(err).Debugf("%s was dropped since it was listed, it has no DDL", table.TableFqn())
				return nil
			}
			if !acceptIncomplete {
				log.WithError(err).Warnf("Could not read the DDL of %s", table.TableFqn())
				return nil
			}
			failures[i] = err
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	var out []*SqlDefinitionRow
	var omissions []Omission
	for i, table := range tables {
		if failures[i] != nil {
			omissions = append(omissions, Omission{
				Database: table.Database, Schema: table.Schema, Table: table.Table, Part: OmittedDefinitions, Err: failures[i],
			})
			continue
		}
		if ddls[i] == "" {
			continue
		}
		table.Sql = ddls[i]
		out = append(out, table)
	}
	return out, omissions, nil
}
