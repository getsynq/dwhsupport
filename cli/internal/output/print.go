package output

import "fmt"

// Print outputs a single item with the given column definitions.
// For structured formats (json/yaml/toon/jq), the full normalized data is output.
// For table/tsv format, columns define which fields to show and how.
func Print(v any, cols Columns) error {
	opts := GetOptions()
	if err := ValidateFormat(opts.Format); err != nil {
		return err
	}
	data, err := NormalizeMap(v)
	if err != nil {
		return fmt.Errorf("output normalize: %w", err)
	}

	switch {
	case opts.JQExpr != "":
		return printJQ(data, opts.JQExpr)
	case opts.Format == FormatJSON:
		return printJSON(data)
	case opts.Format == FormatYAML:
		return printYAML(data)
	case opts.Format == FormatTOON:
		return printTOON(data)
	case opts.Format == FormatTSV:
		return printSingleTSV(data, resolveColumns(cols, opts), opts)
	default:
		return printSingleTable(data, resolveColumns(cols, opts), opts)
	}
}

// PrintList outputs a list of items with the given column definitions.
// For structured formats (json/yaml/toon/jq), the full normalized list is output.
// For table/tsv format, columns define which fields to show and how.
func PrintList[T any](items []T, cols Columns) error {
	opts := GetOptions()
	if err := ValidateFormat(opts.Format); err != nil {
		return err
	}
	rows, err := NormalizeList(items)
	if err != nil {
		return fmt.Errorf("output normalize: %w", err)
	}

	switch {
	case opts.JQExpr != "":
		return printJQ(rows, opts.JQExpr)
	case opts.Format == FormatJSON:
		return printJSON(rows)
	case opts.Format == FormatYAML:
		return printYAML(rows)
	case opts.Format == FormatTOON:
		return printTOON(rows)
	case opts.Format == FormatTSV:
		return printTSV(rows, resolveColumns(cols, opts), opts)
	default:
		return printTable(rows, resolveColumns(cols, opts), opts)
	}
}
