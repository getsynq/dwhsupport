package output

import "fmt"

// Section represents a named output section for multi-section commands.
type Section struct {
	Title   string
	Data    any     // normalized data: map[string]any, []any, or raw value
	Columns Columns // column definitions for table mode; nil for raw display
}

// PrintSections outputs multiple sections.
// In structured formats (JSON/YAML/TOON/jq), all sections are combined into a single object.
// In table/tsv mode, each section renders independently with its heading.
func PrintSections(sections []Section) error {
	opts := GetOptions()
	if err := ValidateFormat(opts.Format); err != nil {
		return err
	}

	if opts.Format == FormatJSON || opts.Format == FormatYAML || opts.Format == FormatTOON || opts.JQExpr != "" {
		combined := make(map[string]any)
		for _, s := range sections {
			combined[s.Title] = s.Data
		}
		if opts.JQExpr != "" {
			return printJQ(combined, opts.JQExpr)
		}
		if opts.Format == FormatYAML {
			return printYAML(combined)
		}
		if opts.Format == FormatTOON {
			return printTOON(combined)
		}
		return printJSON(combined)
	}

	tsv := opts.Format == FormatTSV
	for _, s := range sections {
		fmt.Fprintf(Out, "\n%s:", s.Title)
		if s.Columns != nil {
			resolved := resolveColumns(s.Columns, opts)
			switch data := s.Data.(type) {
			case []any:
				if tsv {
					if err := printTSV(data, resolved, opts); err != nil {
						return err
					}
				} else if err := printTable(data, resolved, opts); err != nil {
					return err
				}
			case map[string]any:
				if tsv {
					if err := printSingleTSV(data, resolved, opts); err != nil {
						return err
					}
				} else if err := printSingleTable(data, resolved, opts); err != nil {
					return err
				}
			}
		} else {
			if err := printJSON(s.Data); err != nil {
				return err
			}
		}
	}
	return nil
}
