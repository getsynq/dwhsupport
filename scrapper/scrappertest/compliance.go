// Package scrappertest provides a compliance test suite that can be embedded
// in warehouse-specific integration test suites to validate that a scrapper
// implementation conforms to the expected contract.
//
// Usage:
//
//	type MyWarehouseSuite struct {
//	    scrappertest.ComplianceSuite
//	}
//
//	func (s *MyWarehouseSuite) SetupSuite() {
//	    s.Scrapper = newMyWarehouseScrapper()
//	}
package scrappertest

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// complianceQueryContext is a realistic query context used by all compliance
// suites to verify that query context propagation (SQL comments, BigQuery labels,
// Snowflake query tags, etc.) works end-to-end without errors.
var complianceQueryContext = querycontext.QueryContext{
	"app":            "synq",
	"workspace":      "test-workspace",
	"integration_id": "int-abc-123",
	"task":           "fetch_monitor_metrics",
	"path":           "monitor::my-project::my_dataset::",
}

// ComplianceSuite validates that a scrapper.Scrapper implementation follows the expected
// contract. Embed this in warehouse-specific integration test suites and set Scrapper
// before tests run (e.g., in SetupSuite).
type ComplianceSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
}

// Ctx returns a background context for use in SetupSuite of embedding suites.
func (s *ComplianceSuite) Ctx() context.Context {
	return context.Background()
}

func (s *ComplianceSuite) ctx() context.Context {
	return querycontext.WithQueryContext(context.Background(), complianceQueryContext)
}

// isAcceptableError returns true if err is nil or ErrUnsupported.
func isAcceptableError(err error) bool {
	return err == nil || errors.Is(err, scrapper.ErrUnsupported)
}

// dbSchemaKey builds a case-insensitive (database, schema) set key. NUL is a
// safe separator: it cannot appear in a valid identifier (reject drops such rows).
func dbSchemaKey(database, schema string) string {
	return strings.ToLower(database) + "\x00" + strings.ToLower(schema)
}

// TestCompliance_HierarchyConsistency verifies the (database, schema, table)
// listings nest correctly — going from the specific up to the general:
//
//   - every (database, schema) returned by QueryTables must be listed by QuerySchemas
//   - every database returned by QueryTables must be listed by QueryDatabases
//     (best-effort: skipped when QueryDatabases is unsupported, or exposes data at
//     a different granularity than QueryTables.Database, e.g. Athena where
//     DatabaseRow.Database is the Glue database rather than the catalog)
//
// This guards against the three list methods drifting apart through divergent
// system-object exclusions, casing, or field mapping.
func (s *ComplianceSuite) TestCompliance_HierarchyConsistency() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}

	ctx := s.ctx()

	tables, err := s.Scrapper.QueryTables(ctx)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("QueryTables unsupported")
	}
	s.Require().NoError(err)
	if len(tables) == 0 {
		s.T().Skip("QueryTables returned no rows")
	}

	// --- Tables ⊆ Schemas, compared on (database, schema) ---
	schemas, err := s.Scrapper.QuerySchemas(ctx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	if err == nil && len(schemas) > 0 {
		schemaSet := make(map[string]bool, len(schemas))
		for _, sc := range schemas {
			schemaSet[dbSchemaKey(sc.Database, sc.Schema)] = true
		}
		for i, t := range tables {
			s.Truef(schemaSet[dbSchemaKey(t.Database, t.Schema)],
				"QueryTables row[%d] %s.%s has no matching QuerySchemas entry", i, t.Database, t.Schema)
		}
	}

	// --- Tables ⊆ Databases, compared on database (best-effort) ---
	databases, err := s.Scrapper.QueryDatabases(ctx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	if err == nil && len(databases) > 0 {
		dbSet := make(map[string]bool, len(databases))
		for _, d := range databases {
			dbSet[strings.ToLower(d.Database)] = true
		}
		// Only enforce when QueryDatabases is database-granular — i.e. its set
		// overlaps the table databases. Warehouses that expose QueryDatabases at
		// schema granularity (Athena) produce zero overlap; skip rather than
		// raise a false failure.
		overlap := false
		for _, t := range tables {
			if dbSet[strings.ToLower(t.Database)] {
				overlap = true
				break
			}
		}
		if overlap {
			for i, t := range tables {
				s.Truef(dbSet[strings.ToLower(t.Database)],
					"QueryTables row[%d] database %s has no matching QueryDatabases entry", i, t.Database)
			}
		} else {
			s.T().Log("skipping Tables⊆Databases check: QueryDatabases granularity does not match QueryTables.Database")
		}
	}
}

// TestCompliance_ValidateConfiguration checks that ValidateConfiguration does not error.
func (s *ComplianceSuite) TestCompliance_ValidateConfiguration() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	_, err := s.Scrapper.ValidateConfiguration(s.ctx())
	s.NoError(err)
}

// TestCompliance_MethodsDoNotError checks that all scrapper methods either succeed or
// return ErrUnsupported, and validates structural invariants and cross-method consistency
// when rows are returned.
func (s *ComplianceSuite) TestCompliance_MethodsDoNotError() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}

	ctx := s.ctx()
	var allInstances []string

	// --- QueryDatabases ---
	databases, err := s.Scrapper.QueryDatabases(ctx)
	if !s.True(isAcceptableError(err), "QueryDatabases error: %v", err) {
		s.T().FailNow()
	}
	for _, row := range databases {
		s.NotEmpty(row.Database)
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- QuerySchemas ---
	schemas, err := s.Scrapper.QuerySchemas(ctx)
	if !s.True(isAcceptableError(err), "QuerySchemas error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range schemas {
		s.NotEmptyf(row.Database, "QuerySchemas row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QuerySchemas row[%d].Schema", i)
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- QueryTables ---
	tables, err := s.Scrapper.QueryTables(ctx)
	if !s.True(isAcceptableError(err), "QueryTables error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range tables {
		s.NotEmptyf(row.Database, "QueryTables row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryTables row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryTables row[%d].Table", i)
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- QueryCatalog ---
	catalog, err := s.Scrapper.QueryCatalog(ctx)
	if !s.True(isAcceptableError(err), "QueryCatalog error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range catalog {
		s.NotEmptyf(row.Database, "QueryCatalog row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryCatalog row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryCatalog row[%d].Table", i)
		s.NotEmptyf(row.Column, "QueryCatalog row[%d].Column", i)
		s.NotEmptyf(row.Type, "QueryCatalog row[%d].Type", i)
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- QueryTableMetrics ---
	metrics, err := s.Scrapper.QueryTableMetrics(ctx, time.Time{})
	if !s.True(isAcceptableError(err), "QueryTableMetrics error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range metrics {
		s.NotEmptyf(row.Database, "QueryTableMetrics row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryTableMetrics row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryTableMetrics row[%d].Table", i)
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- QuerySqlDefinitions ---
	defs, err := s.Scrapper.QuerySqlDefinitions(ctx)
	if !s.True(isAcceptableError(err), "QuerySqlDefinitions error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range defs {
		s.NotEmptyf(row.Database, "QuerySqlDefinitions row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QuerySqlDefinitions row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QuerySqlDefinitions row[%d].Table", i)
		s.NotEmptyf(row.Sql, "QuerySqlDefinitions row[%d].Sql", i)
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- QueryTableConstraints ---
	constraints, err := s.Scrapper.QueryTableConstraints(ctx)
	if !s.True(isAcceptableError(err), "QueryTableConstraints error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range constraints {
		s.NotEmptyf(row.Database, "QueryTableConstraints row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryTableConstraints row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryTableConstraints row[%d].Table", i)
		s.NotEmptyf(row.ConstraintName, "QueryTableConstraints row[%d].ConstraintName", i)
		s.NotEmptyf(row.ConstraintType, "QueryTableConstraints row[%d].ConstraintType", i)
		if row.ConstraintType != scrapper.ConstraintTypeCheck {
			s.NotEmptyf(row.ColumnName, "QueryTableConstraints row[%d].ColumnName", i)
		}
		if row.Instance != "" {
			allInstances = append(allInstances, row.Instance)
		}
	}

	// --- EstimateQuery ---
	// SELECT 1 is valid and plannable on every dialect; the estimate may be
	// zero bytes/rows but must never execute a real scan.
	estimate, err := s.Scrapper.EstimateQuery(ctx, "SELECT 1")
	if !s.True(isAcceptableError(err), "EstimateQuery error: %v", err) {
		s.T().FailNow()
	}
	caps := s.Scrapper.Capabilities().EstimateQuery
	if err == nil {
		s.NotNil(estimate, "EstimateQuery returned nil estimate without an error")
		if estimate != nil {
			// Advertised granularity must line up with what came back.
			if !caps.Bytes {
				s.Nil(estimate.BytesScanned, "EstimateQuery returned bytes but Capabilities.EstimateQuery.Bytes is false")
			}
			if !caps.Rows {
				s.Nil(estimate.Rows, "EstimateQuery returned rows but Capabilities.EstimateQuery.Rows is false")
			}
			s.Equal(caps.Exact, estimate.Exact, "QueryEstimate.Exact must match Capabilities.EstimateQuery.Exact")
		}
	} else if errors.Is(err, scrapper.ErrUnsupported) {
		s.False(caps.Supported, "Capabilities advertises EstimateQuery support but it returned ErrUnsupported")
	}

	// --- Cross-method consistency: all non-empty Instance values must be equal ---
	if len(allInstances) > 1 {
		first := allInstances[0]
		for i, inst := range allInstances[1:] {
			s.Equalf(first, inst, "Instance mismatch across scrapper methods at index %d: got %q, want %q", i+1, inst, first)
		}
	}
}
