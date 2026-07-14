package cmd

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(
		tablesCmd,
		columnsCmd,
		schemasCmd,
		databasesCmd,
		sqlDefinitionsCmd,
		constraintsCmd,
		tableMetricsCmd,
	)
}

var tablesCmd = &cobra.Command{
	Use:   "tables",
	Short: "List tables and views with their type, description and tags",
	Example: "  dwhctl tables --config conn.yaml\n" +
		"  dwhctl tables -c conn.yaml --include analytics.public.'*' -o json",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QueryTables(ctx)
			if err != nil {
				return err
			}
			return emitList("tables", rows, tableColumns)
		})
	},
}

var columnsCmd = &cobra.Command{
	Use:     "columns",
	Aliases: []string{"catalog-columns"},
	Short:   "List column-level catalog metadata (types, positions, comments, tags)",
	Example: "  dwhctl columns --config conn.yaml\n" +
		"  dwhctl columns -c conn.yaml -o toon --wide",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QueryCatalog(ctx)
			if err != nil {
				return err
			}
			return emitList("columns", rows, catalogColumnColumns)
		})
	},
}

var schemasCmd = &cobra.Command{
	Use:     "schemas",
	Short:   "List schemas visible to the connection",
	Example: "  dwhctl schemas --config conn.yaml",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QuerySchemas(ctx)
			if err != nil {
				return err
			}
			return emitList("schemas", rows, schemaColumns)
		})
	},
}

var databasesCmd = &cobra.Command{
	Use:     "databases",
	Short:   "List databases visible to the connection",
	Example: "  dwhctl databases --config conn.yaml",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QueryDatabases(ctx)
			if err != nil {
				return err
			}
			return emitList("databases", rows, databaseColumns)
		})
	},
}

var sqlDefinitionsCmd = &cobra.Command{
	Use:     "sql-definitions",
	Aliases: []string{"sql-defs", "definitions"},
	Short:   "List SQL definitions for views and materialized views",
	Example: "  dwhctl sql-definitions --config conn.yaml --wide",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QuerySqlDefinitions(ctx)
			if err != nil {
				return err
			}
			return emitList("sql definitions", rows, sqlDefinitionColumns)
		})
	},
}

var constraintsCmd = &cobra.Command{
	Use:     "constraints",
	Short:   "List table constraints (keys, indexes, partitioning, clustering)",
	Example: "  dwhctl constraints --config conn.yaml",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			rows, err := s.QueryTableConstraints(ctx)
			if err != nil {
				return err
			}
			return emitList("constraints", rows, constraintColumns)
		})
	},
}
