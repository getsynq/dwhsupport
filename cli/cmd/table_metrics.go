package cmd

import (
	"context"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var flagMetricsSince string

var tableMetricsCmd = &cobra.Command{
	Use:     "table-metrics",
	Aliases: []string{"metrics"},
	Short:   "Fetch metadata-based table metrics (row count, size in bytes, freshness)",
	Long: `Fetch table-level metadata metrics — row count, size in bytes, and last
updated timestamp — sourced from warehouse metadata (information_schema, table
statistics, etc.) rather than by scanning table data.

Use --since to only return tables updated at or after a given time, which lets a
scheduler fetch just what changed. Accepts an RFC3339 timestamp (2026-01-02T15:04:05Z)
or a relative duration (24h, 7d).`,
	Example: "  dwhctl table-metrics --config conn.yaml\n" +
		"  dwhctl table-metrics -c conn.yaml --since 24h -o json",
	RunE: func(cmd *cobra.Command, _ []string) error {
		since, err := parseSince(flagMetricsSince)
		if err != nil {
			return err
		}
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QueryTableMetrics(ctx, since)
			if err != nil {
				return err
			}
			return emitList("table metrics", rows, tableMetricsColumns)
		})
	},
}

func init() {
	tableMetricsCmd.Flags().
		StringVar(&flagMetricsSince, "since", "", "Only tables updated at/after this time (RFC3339 timestamp or duration like 24h, 7d)")
}

// parseSince accepts an empty string (zero time = all tables), an RFC3339
// timestamp, or a Go-style duration (with an added 'd' = 24h shorthand),
// returning the absolute UTC instant to filter from.
func parseSince(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	if d, err := parseDuration(s); err == nil {
		return time.Now().UTC().Add(-d), nil
	}
	return time.Time{}, errors.Errorf("invalid --since %q: want an RFC3339 timestamp or a duration like 24h or 7d", s)
}

// parseDuration extends time.ParseDuration with a 'd' (days) suffix.
func parseDuration(s string) (time.Duration, error) {
	if n := len(s); n > 1 && s[n-1] == 'd' {
		days, err := time.ParseDuration(s[:n-1] + "h")
		if err != nil {
			return 0, err
		}
		return days * 24, nil
	}
	return time.ParseDuration(s)
}
