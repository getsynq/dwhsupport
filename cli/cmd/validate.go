package cmd

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate the connection configuration and report any warnings",
	Long: `Connect to the warehouse and run the scrapper's configuration precheck.
Exits 0 when the configuration is usable (warnings may still be printed), and
non-zero when the connection or configuration is invalid.`,
	Example: "  dwhctl validate --config conn.yaml\n" +
		"  dwhctl validate --config-inline \"$SF_CONN\"",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			warnings, err := s.ValidateConfiguration(ctx)
			result := map[string]any{
				"dialect":  s.DialectType(),
				"valid":    err == nil,
				"warnings": warnings,
			}
			if err != nil {
				result["error"] = err.Error()
			}
			if output.IsStructured() {
				if perr := output.Print(result, validateColumns); perr != nil {
					return perr
				}
			} else {
				if err == nil {
					fmt.Fprintf(output.Out, "✔ %s configuration is valid\n", s.DialectType())
				}
				for _, w := range warnings {
					fmt.Fprintf(output.ErrOut, "warning: %s\n", w)
				}
			}
			return err
		})
	},
}

var validateColumns = output.Columns{
	{Header: "dialect", Path: ".dialect", Default: true},
	{Header: "valid", Path: ".valid", Default: true},
	{Header: "warnings", Path: ".warnings", Default: true},
	{Header: "error", Path: ".error", Default: true},
}

func init() {
	rootCmd.AddCommand(validateCmd)
}
