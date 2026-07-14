package output

import (
	"fmt"

	"github.com/olekukonko/tablewriter"
)

func printTable(rows []any, cols Columns, opts Options) error {
	if len(cols) == 0 {
		return fmt.Errorf("no columns defined for table output")
	}

	headers := make([]string, len(cols))
	for i, col := range cols {
		headers[i] = col.Header
	}

	tw := newTableWriter(headers, opts)

	for _, row := range rows {
		values := make([]string, len(cols))
		for i, col := range cols {
			values[i] = ExtractField(row, col.Path)
		}
		tw.Append(values)
	}

	fmt.Fprintln(Out)
	tw.Render()
	return nil
}

func printSingleTable(data map[string]any, cols Columns, opts Options) error {
	if len(cols) == 0 {
		return fmt.Errorf("no columns defined for table output")
	}

	tw := newTableWriter([]string{"name", "value"}, opts)

	for _, col := range cols {
		tw.Append([]string{col.Header, ExtractField(data, col.Path)})
	}

	fmt.Fprintln(Out)
	tw.Render()
	return nil
}

func newTableWriter(headers []string, opts Options) *tablewriter.Table {
	tw := tablewriter.NewWriter(Out)
	if !opts.NoHeaders {
		tw.SetHeader(headers)
	}
	tw.SetAutoWrapText(false)
	tw.SetAutoFormatHeaders(true)
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetCenterSeparator("")
	tw.SetRowSeparator("-")
	tw.SetColumnSeparator("  ")
	tw.SetBorder(false)
	tw.SetTablePadding("\t")
	return tw
}
