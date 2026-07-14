package output

import "strings"

// Column defines a single table column with its extraction path.
type Column struct {
	Header  string // Display name in table header
	Path    string // jq-style path to extract from JSON, e.g. ".id", ".dwhCoordinates.instance"
	Default bool   // Whether this column is shown by default in table mode
}

// Columns is an ordered list of column definitions.
type Columns []Column

// resolveColumns filters columns based on the current output options.
//   - If explicit column names are provided, only those columns are returned
//     (matched by header, case-insensitive; unknown names are ignored).
//   - If wide mode is enabled, all columns are returned.
//   - Otherwise, only default columns are returned.
func resolveColumns(cols Columns, opts Options) Columns {
	if len(opts.Columns) > 0 {
		selected := make(Columns, 0, len(opts.Columns))
		for _, name := range opts.Columns {
			for _, col := range cols {
				if strings.EqualFold(col.Header, name) {
					selected = append(selected, col)
					break
				}
			}
		}
		return selected
	}
	if opts.Wide {
		return cols
	}
	defaults := make(Columns, 0, len(cols))
	for _, col := range cols {
		if col.Default {
			defaults = append(defaults, col)
		}
	}
	return defaults
}
