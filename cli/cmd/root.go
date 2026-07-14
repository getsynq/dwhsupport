package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// version is stamped at build time via -ldflags "-X .../cmd.version=...".
var version = "dev"

var (
	logLevel   string
	verbose    bool
	outputFile string

	// closeOutput is set by PersistentPreRunE when --output-file is used and is
	// invoked in PersistentPostRunE to flush and restore stdout.
	closeOutput func() error
)

var rootCmd = &cobra.Command{
	Use:   "dwhctl",
	Short: "Query data-warehouse catalog & metadata through the SYNQ scrapper interface",
	Long: `dwhctl is a universal command-line interface over the SYNQ dwhsupport
Scrapper interface. It connects to any supported warehouse (Snowflake, BigQuery,
Databricks, Postgres, Redshift, ClickHouse, DuckDB, MySQL, Trino, Oracle, MSSQL,
Athena, Fabric) and extracts catalog and metadata metrics with a single binary —
no warehouse-specific tooling required.

Every command reads a connection config (from --config, --config-inline, stdin,
or $DWHCTL_CONFIG) and prints structured results. Output is a human-readable
table by default, or a machine-readable format (json, yaml, toon, tsv) via -o.
Diagnostics go to stderr, results go to stdout (or --output-file), so output can
be piped or redirected safely.

  # human, at a terminal
  dwhctl tables --config conn.yaml

  # machine, inline config from an env var, JSON to a file
  dwhctl catalog --config-inline "$SF_CONN" -o json --output-file catalog.json

  # scope to a subset of the warehouse
  dwhctl columns --config conn.yaml --include analytics.public.'*'`,
	SilenceUsage:      true,
	SilenceErrors:     true,
	Version:           version,
	PersistentPreRunE: setup,
	PersistentPostRunE: func(*cobra.Command, []string) error {
		if closeOutput != nil {
			return closeOutput()
		}
		return nil
	},
}

func setup(cmd *cobra.Command, _ []string) error {
	// Logs always go to stderr so stdout stays a clean machine-readable stream.
	logrus.SetOutput(os.Stderr)
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, DisableLevelTruncation: true})

	lvl := logrus.WarnLevel
	if verbose {
		lvl = logrus.InfoLevel
	}
	if logLevel != "" {
		parsed, err := logrus.ParseLevel(logLevel)
		if err != nil {
			return fmt.Errorf("invalid --log-level %q: %w", logLevel, err)
		}
		lvl = parsed
	}
	logrus.SetLevel(lvl)

	if err := output.ValidateFormat(output.GetOptions().Format); err != nil {
		return err
	}

	closer, err := output.UseFile(outputFile)
	if err != nil {
		return err
	}
	closeOutput = closer
	return nil
}

// ExecuteContext runs the root command with the given context (for signal
// cancellation). It is the single entry point from main.
func ExecuteContext(ctx context.Context) {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(1)
	}
}

func init() {
	output.RegisterFlags(rootCmd)
	rootCmd.PersistentFlags().
		StringVarP(&outputFile, "output-file", "O", "", "Write results to this file instead of stdout (logs still go to stderr)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "", "Log level: trace, debug, info, warn, error (default warn)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging (info level); shorthand for --log-level info")

	registerConnectionFlags(rootCmd)
}
