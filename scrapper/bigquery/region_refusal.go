package bigquery

import (
	"context"
	"sync/atomic"

	"github.com/getsynq/dwhsupport/logging"
)

// regionRefusal remembers that the role was refused a view of the region's
// INFORMATION_SCHEMA. A role granted only on the datasets is refused it on
// every call, and each refusal is a failed job in the customer's audit log, so
// once it was refused the scrapper reads each dataset's view instead for the
// rest of its life. The zero value has seen no refusal.
//
// Each view has its own: INFORMATION_SCHEMA.TABLES needs bigquery.tables.list
// and bigquery.tables.get on the project, ROUTINES needs bigquery.routines.list
// and bigquery.routines.get, and a role may hold one pair and not the other.
type regionRefusal struct {
	refused atomic.Bool
}

// known reports whether a read of the region's view was refused before.
func (r *regionRefusal) known() bool {
	return r.refused.Load()
}

// remember reports whether err refused the read of the region's view, and
// remembers it if so. Only an error a grant would fix counts: a quota or rate
// limit, which BigQuery also answers with a 403, is not remembered, and the
// caller returns it rather than read each dataset, which would run many more
// jobs against the same quota.
func (r *regionRefusal) remember(ctx context.Context, view string, err error) bool {
	if !isRefused(err) {
		return false
	}
	if r.refused.CompareAndSwap(false, true) {
		logging.GetLogger(ctx).WithError(err).
			Infof("region INFORMATION_SCHEMA.%s refused, reading each dataset's from now on", view)
	}
	return true
}
