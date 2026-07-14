package cmd

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

var flagShapeSQL string

var shapeCmd = &cobra.Command{
	Use:   "shape [SQL]",
	Short: "Describe the column shape of a SELECT without returning rows",
	Long: `Return the output columns (name, native type, position) a SELECT would
produce, determined from warehouse metadata without materializing the result.
The SQL can be a positional argument, --sql, or stdin ('-').`,
	Example: "  dwhctl shape 'SELECT * FROM analytics.public.orders' --config conn.yaml",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		sql, err := resolveSQL(flagShapeSQL, args)
		if err != nil {
			return err
		}
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			cols, err := s.QueryShape(ctx, sql)
			if err != nil {
				return err
			}
			return emitList("columns", cols, shapeColumns)
		})
	},
}

func init() {
	shapeCmd.Flags().StringVar(&flagShapeSQL, "sql", "", "SQL SELECT to describe (alternative to a positional argument or stdin)")
	rootCmd.AddCommand(shapeCmd)
}
