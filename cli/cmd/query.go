package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

var (
	flagQuerySQL   string
	flagQueryLimit int
)

var queryCmd = &cobra.Command{
	Use:   "query [SQL]",
	Short: "Run an arbitrary SELECT and stream the rows",
	Long: `Execute a user-supplied SELECT against the warehouse and print the
resulting rows. The SQL can be passed as a positional argument, via --sql, or on
stdin (use '-'). Results stream, and --limit caps how many rows are returned
(default 100; 0 means unlimited).

This is a generic data-preview surface: string columns are preserved as text and
NULLs are rendered as empty cells.`,
	Example: "  dwhctl query 'SELECT 1 AS n' --config conn.yaml\n" +
		"  dwhctl query --sql 'SELECT * FROM analytics.public.orders' -c conn.yaml --limit 20 -o json\n" +
		"  echo 'SELECT current_date' | dwhctl query - -c conn.yaml",
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		sql, err := resolveSQL(flagQuerySQL, args)
		if err != nil {
			return err
		}
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			iter, err := s.RunRawQuery(ctx, sql)
			if err != nil {
				return err
			}
			defer iter.Close()

			cols := iter.Columns()
			outCols := make(output.Columns, len(cols))
			for i, c := range cols {
				outCols[i] = output.Column{Header: c.Name, Path: "." + jqField(c.Name), Default: true}
			}

			var rows []map[string]any
			for {
				if flagQueryLimit > 0 && len(rows) >= flagQueryLimit {
					fmt.Fprintf(output.ErrOut, "row limit %d reached; pass --limit 0 for all rows\n", flagQueryLimit)
					break
				}
				vals, err := iter.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				row := make(map[string]any, len(vals))
				for _, v := range vals {
					row[v.Name] = columnValueToAny(v)
				}
				rows = append(rows, row)
			}
			return emitList("rows", rows, outCols)
		})
	},
}

// resolveSQL picks the SQL from --sql, the first positional arg, or stdin ('-').
func resolveSQL(flag string, args []string) (string, error) {
	if flag != "" {
		return flag, nil
	}
	if len(args) > 0 {
		if args[0] == "-" {
			b, err := io.ReadAll(os.Stdin)
			if err != nil {
				return "", err
			}
			return string(b), nil
		}
		return args[0], nil
	}
	return "", fmt.Errorf("no SQL provided: pass it as an argument, via --sql, or on stdin with '-'")
}

// columnValueToAny converts a scrapper cell into a plain JSON-friendly value.
func columnValueToAny(v *scrapper.ColumnValue) any {
	if v == nil || v.IsNull || v.Value == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case scrapper.StringValue:
		return string(val)
	case scrapper.JsonValue:
		// Decode the canonical JSON so structured renderers (json/yaml/toon)
		// nest it, rather than emitting a JSON string. Fall back to the raw
		// text if it somehow fails to parse.
		var decoded any
		if err := json.Unmarshal([]byte(val), &decoded); err == nil {
			return decoded
		}
		return string(val)
	case scrapper.IntValue:
		return int64(val)
	case scrapper.DoubleValue:
		return float64(val)
	case scrapper.TimeValue:
		return time.Time(val)
	case *scrapper.BigIntValue:
		return val.String()
	case scrapper.IgnoredValue:
		return nil
	default:
		if bi, ok := v.Value.(interface{ BigInt() *big.Int }); ok {
			return bi.BigInt().String()
		}
		return fmt.Sprintf("%v", v.Value)
	}
}

// jqField renders a column name as a gojq object key. Simple identifiers are
// used bare (.name); anything else is bracket-quoted (.["odd name"]) so the
// table/tsv extractor can still resolve it.
func jqField(name string) string {
	simple := name != ""
	for _, r := range name {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			simple = false
			break
		}
	}
	if simple {
		return name
	}
	return `["` + strings.ReplaceAll(name, `"`, `\"`) + `"]`
}

func init() {
	queryCmd.Flags().StringVar(&flagQuerySQL, "sql", "", "SQL SELECT to execute (alternative to a positional argument or stdin)")
	queryCmd.Flags().IntVar(&flagQueryLimit, "limit", 100, "Maximum rows to return (0 = unlimited)")
	rootCmd.AddCommand(queryCmd)
}
