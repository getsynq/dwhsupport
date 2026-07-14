package cmd

import (
	"context"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Fetch the full catalog: tables, columns, SQL definitions and constraints",
	Long: `Fetch a complete catalog snapshot for the connection in one command,
mirroring what the Coalesce Quality agent collects: the table/view list, column metadata,
view SQL definitions, and table constraints. The four passes run concurrently.

In structured formats (json/yaml/toon/jq) the result is a single object with
tables, columns, sql_definitions and constraints keys — convenient for feeding
another tool. In table mode each section is printed in turn.

Constraints are collected inline with tables when the dialect supports it,
avoiding a redundant pass.`,
	Example: "  dwhctl catalog --config conn.yaml -o json --output-file catalog.json\n" +
		"  dwhctl catalog -c conn.yaml --include analytics.'*'.'*'",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			var (
				tables      []*scrapper.TableRow
				columns     []*scrapper.CatalogColumnRow
				sqlDefs     []*scrapper.SqlDefinitionRow
				constraints []*scrapper.TableConstraintRow
			)

			caps := s.Capabilities()
			var tableOpts []scrapper.QueryTablesOption
			if caps.ConstraintsViaQueryTables {
				tableOpts = append(tableOpts, scrapper.WithConstraints())
			}

			g, gctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				var err error
				tables, err = s.QueryTables(gctx, tableOpts...)
				return err
			})
			g.Go(func() error {
				cols, err := s.QueryCatalog(gctx)
				if err != nil {
					return tolerateUnsupported(err)
				}
				columns = cols
				return nil
			})
			g.Go(func() error {
				sd, err := s.QuerySqlDefinitions(gctx)
				if err != nil {
					return tolerateUnsupported(err)
				}
				sqlDefs = sd
				return nil
			})
			// Only issue a dedicated constraints pass when it isn't already
			// folded into QueryTables. ErrUnsupported is tolerated.
			if !caps.ConstraintsViaQueryTables {
				g.Go(func() error {
					tc, err := s.QueryTableConstraints(gctx)
					if err != nil {
						return tolerateUnsupported(err)
					}
					constraints = tc
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
			if caps.ConstraintsViaQueryTables {
				for _, t := range tables {
					constraints = append(constraints, t.Constraints...)
				}
			}

			return printCatalogSections(tables, columns, sqlDefs, constraints)
		})
	},
}

// tolerateUnsupported maps scrapper.ErrUnsupported to nil so a dialect that
// doesn't implement one catalog pass leaves that section empty instead of
// failing the whole snapshot. Any other error is propagated.
func tolerateUnsupported(err error) error {
	if errors.Is(err, scrapper.ErrUnsupported) {
		return nil
	}
	return err
}

func printCatalogSections(
	tables []*scrapper.TableRow,
	columns []*scrapper.CatalogColumnRow,
	sqlDefs []*scrapper.SqlDefinitionRow,
	constraints []*scrapper.TableConstraintRow,
) error {
	tablesData, err := output.NormalizeList(tables)
	if err != nil {
		return err
	}
	columnsData, err := output.NormalizeList(columns)
	if err != nil {
		return err
	}
	sqlDefsData, err := output.NormalizeList(sqlDefs)
	if err != nil {
		return err
	}
	constraintsData, err := output.NormalizeList(constraints)
	if err != nil {
		return err
	}
	return output.PrintSections([]output.Section{
		{Title: "tables", Data: tablesData, Columns: tableColumns},
		{Title: "columns", Data: columnsData, Columns: catalogColumnColumns},
		{Title: "sql_definitions", Data: sqlDefsData, Columns: sqlDefinitionColumns},
		{Title: "constraints", Data: constraintsData, Columns: constraintColumns},
	})
}

func init() {
	rootCmd.AddCommand(catalogCmd)
}
