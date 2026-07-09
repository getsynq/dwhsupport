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

// pgExplainNode mirrors the top-level plan node of `EXPLAIN (FORMAT JSON)`.
// "Plan Rows" is the estimated number of rows the top node emits.
type pgExplainNode struct {
	Plan struct {
		PlanRows float64 `json:"Plan Rows"`
	} `json:"Plan"`
}

func parsePostgresEstimate(jsonText string) (*scrapper.QueryEstimate, error) {
	var nodes []pgExplainNode
	if err := json.Unmarshal([]byte(jsonText), &nodes); err != nil {
		return nil, errors.Wrap(err, "parsing EXPLAIN (FORMAT JSON) output")
	}
	if len(nodes) == 0 {
		return nil, errors.New("EXPLAIN (FORMAT JSON) output had no plan")
	}
	rows := int64(nodes[0].Plan.PlanRows)
	return &scrapper.QueryEstimate{Rows: &rows}, nil
}
