package output

import "fmt"

// Format represents the output format.
type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
	FormatYAML  Format = "yaml"
	FormatTOON  Format = "toon"
	FormatTSV   Format = "tsv"
	FormatWide  Format = "wide"
)

// knownFormats lists every value accepted by the --output flag.
var knownFormats = []Format{FormatTable, FormatJSON, FormatYAML, FormatTOON, FormatTSV, FormatWide}

// Options holds the resolved output settings from flags.
type Options struct {
	Format    Format
	JQExpr    string
	Columns   []string
	Wide      bool
	NoHeaders bool
}

// ValidateFormat returns an error if f is not a recognised output format.
func ValidateFormat(f Format) error {
	for _, k := range knownFormats {
		if f == k {
			return nil
		}
	}
	return fmt.Errorf("unknown output format %q (valid: table, json, yaml, toon, tsv, wide)", f)
}
