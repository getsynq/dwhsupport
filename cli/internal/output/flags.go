package output

import "github.com/spf13/cobra"

var globalOpts Options

// DefaultFormat returns the format used when the user does not pass -o. When the
// CLI is invoked by an AI agent (see IsAgentContext) we default to TOON, which
// is ~2x more token-efficient than JSON for tabular data; otherwise a human is
// at the terminal and gets a table. An explicit -o always overrides this.
func DefaultFormat() Format {
	if IsAgentContext() {
		return FormatTOON
	}
	return FormatTable
}

// RegisterFlags adds the output-related persistent flags on the given command,
// defaulting to table for humans and TOON for detected agent contexts.
func RegisterFlags(cmd *cobra.Command) {
	RegisterFlagsWithDefault(cmd, DefaultFormat())
}

// RegisterFlagsWithDefault is like RegisterFlags but lets the caller pin the
// default output format explicitly.
func RegisterFlagsWithDefault(cmd *cobra.Command, def Format) {
	if def == "" {
		def = FormatTable
	}
	cmd.PersistentFlags().
		StringVarP((*string)(&globalOpts.Format), "output", "o", string(def), "Output format: table, json, yaml, toon, tsv, wide")
	cmd.PersistentFlags().StringVar(&globalOpts.JQExpr, "jq", "", "jq expression to filter JSON output")
	cmd.PersistentFlags().StringSliceVar(&globalOpts.Columns, "columns", nil, "Comma-separated list of columns to display (table/tsv mode)")
	cmd.PersistentFlags().BoolVar(&globalOpts.Wide, "wide", false, "Show all columns in table mode")
	cmd.PersistentFlags().BoolVar(&globalOpts.NoHeaders, "no-headers", false, "Omit the header row (table/tsv mode)")

	_ = cmd.RegisterFlagCompletionFunc("output", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json", "yaml", "toon", "tsv", "wide"}, cobra.ShellCompDirectiveNoFileComp
	})
}

// GetOptions returns the current resolved options.
func GetOptions() Options {
	opts := globalOpts
	if opts.Format == "" {
		opts.Format = FormatTable
	}
	if opts.Format == FormatWide {
		opts.Format = FormatTable
		opts.Wide = true
	}
	if opts.JQExpr != "" && opts.Format == FormatTable {
		opts.Format = FormatJSON
	}
	return opts
}

// IsStructured returns true when the output format is machine-oriented
// (json, yaml, toon, tsv, or jq filtering) — i.e. human headlines should be
// suppressed and routed to stderr instead.
func IsStructured() bool {
	opts := GetOptions()
	return opts.Format == FormatJSON || opts.Format == FormatYAML || opts.Format == FormatTOON ||
		opts.Format == FormatTSV || opts.JQExpr != ""
}
