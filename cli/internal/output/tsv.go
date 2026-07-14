package output

import (
	"fmt"
	"strings"
)

// printTSV renders a list as tab-separated values — one row per item, columns
// in definition order. Cell values have tabs and newlines stripped so each
// record stays on a single line, which keeps the output safe for `cut`, `awk`,
// and spreadsheet import.
func printTSV(rows []any, cols Columns, opts Options) error {
	if len(cols) == 0 {
		return fmt.Errorf("no columns defined for tsv output")
	}
	if !opts.NoHeaders {
		headers := make([]string, len(cols))
		for i, col := range cols {
			headers[i] = col.Header
		}
		fmt.Fprintln(Out, strings.Join(headers, "\t"))
	}
	for _, row := range rows {
		values := make([]string, len(cols))
		for i, col := range cols {
			values[i] = sanitizeTSV(ExtractField(row, col.Path))
		}
		fmt.Fprintln(Out, strings.Join(values, "\t"))
	}
	return nil
}

// printSingleTSV renders a single object as two columns: header and value.
func printSingleTSV(data map[string]any, cols Columns, opts Options) error {
	if len(cols) == 0 {
		return fmt.Errorf("no columns defined for tsv output")
	}
	if !opts.NoHeaders {
		fmt.Fprintln(Out, "name\tvalue")
	}
	for _, col := range cols {
		fmt.Fprintf(Out, "%s\t%s\n", col.Header, sanitizeTSV(ExtractField(data, col.Path)))
	}
	return nil
}

func sanitizeTSV(s string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
}
