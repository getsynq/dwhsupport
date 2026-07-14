package cmd

import (
	"context"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

var flagEstimateSQL string

var estimateCmd = &cobra.Command{
	Use:   "estimate [SQL]",
	Short: "Estimate the scan size of a SELECT without executing it",
	Long: `Return a pre-execution estimate of what a SELECT would scan — bytes
and/or a planner row estimate — from engine metadata (dry-run / EXPLAIN). The
query is never executed. Not every dialect can estimate; unsupported dialects
report so rather than failing.`,
	Example: "  dwhctl estimate 'SELECT * FROM analytics.public.orders' --config conn.yaml -o json",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		sql, err := resolveSQL(flagEstimateSQL, args)
		if err != nil {
			return err
		}
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			caps := s.Capabilities().EstimateQuery
			if !caps.Supported {
				return output.Print(map[string]any{
					"dialect":   s.DialectType(),
					"supported": false,
				}, estimateColumns)
			}
			est, err := s.EstimateQuery(ctx, sql)
			if err != nil {
				return err
			}
			result := map[string]any{
				"dialect":       s.DialectType(),
				"supported":     true,
				"bytes_scanned": est.BytesScanned,
				"rows":          est.Rows,
				"exact":         est.Exact,
			}
			return output.Print(result, estimateColumns)
		})
	},
}

var estimateColumns = output.Columns{
	{Header: "dialect", Path: ".dialect", Default: true},
	{Header: "supported", Path: ".supported", Default: true},
	{Header: "bytes_scanned", Path: ".bytes_scanned", Default: true},
	{Header: "rows", Path: ".rows", Default: true},
	{Header: "exact", Path: ".exact", Default: true},
}

func init() {
	estimateCmd.Flags().StringVar(&flagEstimateSQL, "sql", "", "SQL SELECT to estimate (alternative to a positional argument or stdin)")
	rootCmd.AddCommand(estimateCmd)
}
