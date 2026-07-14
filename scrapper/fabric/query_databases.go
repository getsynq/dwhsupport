package fabric

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// QueryDatabases lists the workspace databases in scope. The Fabric SQL endpoint
// is workspace-shared, so this returns every warehouse/lakehouse the connection
// can see (via sys.databases), narrowed by conf.Databases and the context scope
// filter — the same set the other scrapper methods iterate.
func (e *FabricScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]*scrapper.DatabaseRow, 0, len(databases))
	for _, db := range databases {
		out = append(out, &scrapper.DatabaseRow{Instance: e.conf.Host, Database: db})
	}
	return out, nil
}
