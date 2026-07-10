package snowflake

import (
	"context"
	"encoding/json"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
	"github.com/pkg/errors"
)

// EstimateQuery returns a bytes estimate via `EXPLAIN USING JSON`, which
// compiles the query and reports the partitions/bytes the optimizer expects to
// scan without executing it. The GlobalStats.bytesAssigned figure is a
// planner estimate derived from micro-partition metadata, not an authoritative
// scan count, so Exact stays false. Row counts are not reported.
func (e *SnowflakeScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	_, rows, err := scrapperstdsql.ExplainRows(ctx, e.executor, "EXPLAIN USING JSON "+sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, errors.New("EXPLAIN USING JSON returned no rows")
	}
	jsonText, ok := scrapperstdsql.CellString(rows[0][0])
	if !ok {
		return nil, errors.New("EXPLAIN USING JSON returned a null plan")
	}
	return parseSnowflakeEstimate(jsonText)
}

// sfExplainPlan mirrors the JSON document returned by `EXPLAIN USING JSON`. Only
// GlobalStats.bytesAssigned is needed: it is the total bytes the optimizer
// assigned across all scanned micro-partitions.
type sfExplainPlan struct {
	GlobalStats struct {
		BytesAssigned int64 `json:"bytesAssigned"`
	} `json:"GlobalStats"`
}

func parseSnowflakeEstimate(jsonText string) (*scrapper.QueryEstimate, error) {
	var plan sfExplainPlan
	if err := json.Unmarshal([]byte(jsonText), &plan); err != nil {
		return nil, errors.Wrap(err, "parsing EXPLAIN USING JSON output")
	}
	bytes := plan.GlobalStats.BytesAssigned
	return &scrapper.QueryEstimate{BytesScanned: &bytes}, nil
}
