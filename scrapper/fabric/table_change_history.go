package fabric

import (
	"context"
	"strconv"
	"time"

	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

var _ scrapper.TableChangeHistoryProvider = &FabricScrapper{}

// FetchTableChangeHistory returns the commits of the table's Delta log in
// OneLake that changed data and were written in [from, to], newest first, at
// most limit of them. The version is the Delta version of the commit. A commit
// that only compacts files or changes the table metadata is not an event.
//
// It fails for a connection that cannot reach OneLake, for a database that no
// single storage item of the workspace names, and for a table with no Delta log.
func (e *FabricScrapper) FetchTableChangeHistory(
	ctx context.Context,
	fqn scrapper.DwhFqn,
	from, to time.Time,
	limit int,
) ([]*scrapper.TableChangeEvent, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	reader, err := e.deltaLog.Get()
	if err != nil {
		return nil, err
	}
	itemID, ok, err := reader.itemOf(ctx, fqn.DatabaseName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("no single storage item of the workspace is named %q", fqn.DatabaseName)
	}
	commits, err := reader.history(ctx, itemID, tableDirs(nil, fqn.SchemaName, fqn.ObjectName), from, to, limit)
	if err != nil {
		return nil, errors.Wrapf(err, "reading the Delta log of %s.%s.%s", fqn.DatabaseName, fqn.SchemaName, fqn.ObjectName)
	}

	events := make([]*scrapper.TableChangeEvent, 0, len(commits))
	for _, c := range commits {
		events = append(events, &scrapper.TableChangeEvent{
			Timestamp:    c.Time,
			Version:      strconv.FormatInt(c.Version, 10),
			Operation:    c.Operation,
			RowsInserted: c.RowsInserted,
			RowsUpdated:  c.RowsUpdated,
			RowsDeleted:  c.RowsDeleted,
		})
	}
	collector.SetRowsProduced(int64(len(events)))
	return events, nil
}
