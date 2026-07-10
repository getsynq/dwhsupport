package postgres

import (
	"context"
	"encoding/json"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
	"github.com/pkg/errors"
)

// EstimateQuery returns a row estimate via `EXPLAIN (FORMAT JSON)`, which plans
// the query without executing it (no ANALYZE). Postgres reports a planner row
// estimate but no scan-bytes figure, so only Rows is populated. The estimate is
// only as good as the table's last ANALYZE and can be off by orders of
// magnitude on stale statistics.
//
// Rows is the largest per-node "Plan Rows" across the whole plan tree, not the
// top node's output. The top node emits post-aggregation cardinality — 1 for
// `SELECT count(*) FROM big_table` — which is useless as a scan-size warning;
// the deepest scan node carries the figure a caller actually wants to warn on.
func (e *PostgresScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	_, rows, err := scrapperstdsql.ExplainRows(ctx, e.executor, "EXPLAIN (FORMAT JSON) "+sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, errors.New("EXPLAIN (FORMAT JSON) returned no rows")
	}
	jsonText, ok := scrapperstdsql.CellString(rows[0][0])
	if !ok {
		return nil, errors.New("EXPLAIN (FORMAT JSON) returned a null plan")
	}
	return parsePostgresEstimate(jsonText)
}

// pgExplainRoot mirrors one element of the `EXPLAIN (FORMAT JSON)` array.
type pgExplainRoot struct {
	Plan pgPlanNode `json:"Plan"`
}

// pgPlanNode is a plan node; "Plan Rows" is the estimated rows the node emits
// (post-filter, post-aggregation) and "Plans" holds its children.
type pgPlanNode struct {
	PlanRows float64      `json:"Plan Rows"`
	Plans    []pgPlanNode `json:"Plans"`
}

func parsePostgresEstimate(jsonText string) (*scrapper.QueryEstimate, error) {
	var roots []pgExplainRoot
	if err := json.Unmarshal([]byte(jsonText), &roots); err != nil {
		return nil, errors.Wrap(err, "parsing EXPLAIN (FORMAT JSON) output")
	}
	if len(roots) == 0 {
		return nil, errors.New("EXPLAIN (FORMAT JSON) output had no plan")
	}
	rows := int64(maxPlanRows(roots[0].Plan))
	return &scrapper.QueryEstimate{Rows: &rows}, nil
}

// maxPlanRows returns the largest "Plan Rows" in the subtree rooted at n. The
// deepest scan node's estimate is a better scan-size proxy than the top node's
// output cardinality, which collapses to 1 for aggregates.
func maxPlanRows(n pgPlanNode) float64 {
	max := n.PlanRows
	for _, child := range n.Plans {
		if m := maxPlanRows(child); m > max {
			max = m
		}
	}
	return max
}
