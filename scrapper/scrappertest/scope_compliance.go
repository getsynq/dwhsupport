package scrappertest

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/suite"
)

// ScopeComplianceSuite validates that scope filtering works correctly for a scrapper
// implementation. It first queries unscoped results to discover real data, then applies
// include/exclude scope filters and verifies that returned rows match the scope.
//
// Embed this in warehouse-specific integration test suites alongside ComplianceSuite.
// Set Scrapper before tests run (e.g., in SetupSuite).
//
// Usage:
//
//	type MyWarehouseSuite struct {
//	    scrappertest.ScopeComplianceSuite
//	}
//
//	func (s *MyWarehouseSuite) SetupSuite() {
//	    s.Scrapper = newMyWarehouseScrapper()
//	}
type ScopeComplianceSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
}

// Ctx returns a background context for use in SetupSuite of embedding suites.
func (s *ScopeComplianceSuite) Ctx() context.Context {
	return context.Background()
}

func (s *ScopeComplianceSuite) ctx() context.Context {
	return context.Background()
}

// TestScopeCompliance_ExcludeFilterReducesResults applies an exclude filter for a schema
// found in the unscoped results and verifies that the filtered results contain fewer rows
// and none from the excluded schema.
func (s *ScopeComplianceSuite) TestScopeCompliance_ExcludeFilterReducesResults() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}

	ctx := s.ctx()

	// Get unscoped tables to discover a schema we can exclude.
	tables, err := s.Scrapper.QueryTables(ctx)
	if errors.Is(err, scrapper.ErrUnsupported) || len(tables) == 0 {
		s.T().Skip("QueryTables unsupported or returned no rows")
	}
	s.Require().NoError(err)

	// Pick the first schema we find.
	targetDB := tables[0].Database
	targetSchema := tables[0].Schema

	// Count how many tables are in that schema.
	schemaCount := 0
	for _, t := range tables {
		if strings.EqualFold(t.Database, targetDB) && strings.EqualFold(t.Schema, targetSchema) {
			schemaCount++
		}
	}
	if schemaCount == len(tables) {
		s.T().Skip("All tables are in a single schema, cannot test exclude filter reduction")
	}

	// Apply exclude filter and query again.
	filter := &scope.ScopeFilter{
		Exclude: []scope.ScopeRule{
			{Database: targetDB, Schema: targetSchema},
		},
	}
	scopedCtx := scope.WithScope(ctx, filter)

	filteredTables, err := s.Scrapper.QueryTables(scopedCtx)
	s.Require().NoError(err)

	s.Less(len(filteredTables), len(tables),
		"Excluding schema %s.%s should reduce the number of tables", targetDB, targetSchema)

	for _, t := range filteredTables {
		if strings.EqualFold(t.Database, targetDB) {
			s.False(strings.EqualFold(t.Schema, targetSchema),
				"QueryTables should not return rows from excluded schema %s.%s, got table %s",
				targetDB, targetSchema, t.Table)
		}
	}
}

// TestScopeCompliance_IncludeFilterLimitsResults applies an include filter for a single
// schema and verifies that all returned rows belong to that schema.
func (s *ScopeComplianceSuite) TestScopeCompliance_IncludeFilterLimitsResults() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}

	ctx := s.ctx()

	// Get unscoped catalog to discover a schema.
	catalog, err := s.Scrapper.QueryCatalog(ctx)
	if errors.Is(err, scrapper.ErrUnsupported) || len(catalog) == 0 {
		s.T().Skip("QueryCatalog unsupported or returned no rows")
	}
	s.Require().NoError(err)

	targetDB := catalog[0].Database
	targetSchema := catalog[0].Schema

	// Apply include filter.
	filter := &scope.ScopeFilter{
		Include: []scope.ScopeRule{
			{Database: targetDB, Schema: targetSchema},
		},
	}
	scopedCtx := scope.WithScope(ctx, filter)

	filteredCatalog, err := s.Scrapper.QueryCatalog(scopedCtx)
	s.Require().NoError(err)
	s.NotEmpty(filteredCatalog, "Include filter for %s.%s should return at least the target schema", targetDB, targetSchema)

	for i, row := range filteredCatalog {
		s.Truef(strings.EqualFold(row.Database, targetDB) && strings.EqualFold(row.Schema, targetSchema),
			"QueryCatalog row[%d] should match include scope %s.%s, got %s.%s.%s",
			i, targetDB, targetSchema, row.Database, row.Schema, row.Table)
	}
}

// TestScopeCompliance_NonMatchingScopeReturnsEmpty applies a scope filter that matches
// nothing and verifies that all filtered methods return empty results.
func (s *ScopeComplianceSuite) TestScopeCompliance_NonMatchingScopeReturnsEmpty() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}

	ctx := s.ctx()

	// Use an include rule that should never match any real data.
	filter := &scope.ScopeFilter{
		Include: []scope.ScopeRule{
			{Database: "__synq_nonexistent_db_scope_test__", Schema: "__synq_nonexistent_schema__"},
		},
	}
	scopedCtx := scope.WithScope(ctx, filter)

	tables, err := s.Scrapper.QueryTables(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	s.Empty(tables, "QueryTables with non-matching scope should return empty")

	catalog, err := s.Scrapper.QueryCatalog(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	s.Empty(catalog, "QueryCatalog with non-matching scope should return empty")

	metrics, err := s.Scrapper.QueryTableMetrics(scopedCtx, time.Time{})
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	s.Empty(metrics, "QueryTableMetrics with non-matching scope should return empty")

	constraints, err := s.Scrapper.QueryTableConstraints(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	s.Empty(constraints, "QueryTableConstraints with non-matching scope should return empty")
}

// TestScopeCompliance_AllFilteredMethodsRespectScope applies an include filter and
// verifies that every filtered method only returns rows matching the scope.
func (s *ScopeComplianceSuite) TestScopeCompliance_AllFilteredMethodsRespectScope() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}

	ctx := s.ctx()

	// Get unscoped tables to discover a schema.
	tables, err := s.Scrapper.QueryTables(ctx)
	if errors.Is(err, scrapper.ErrUnsupported) || len(tables) == 0 {
		s.T().Skip("QueryTables unsupported or returned no rows")
	}
	s.Require().NoError(err)

	targetDB := tables[0].Database
	targetSchema := tables[0].Schema

	filter := &scope.ScopeFilter{
		Include: []scope.ScopeRule{
			{Database: targetDB, Schema: targetSchema},
		},
	}
	scopedCtx := scope.WithScope(ctx, filter)

	// Check QueryTables
	filteredTables, err := s.Scrapper.QueryTables(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	for i, row := range filteredTables {
		s.Truef(strings.EqualFold(row.Database, targetDB) && strings.EqualFold(row.Schema, targetSchema),
			"QueryTables row[%d]: expected %s.%s, got %s.%s", i, targetDB, targetSchema, row.Database, row.Schema)
	}

	// Check QueryCatalog
	filteredCatalog, err := s.Scrapper.QueryCatalog(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	for i, row := range filteredCatalog {
		s.Truef(strings.EqualFold(row.Database, targetDB) && strings.EqualFold(row.Schema, targetSchema),
			"QueryCatalog row[%d]: expected %s.%s, got %s.%s", i, targetDB, targetSchema, row.Database, row.Schema)
	}

	// Check QueryTableMetrics
	filteredMetrics, err := s.Scrapper.QueryTableMetrics(scopedCtx, time.Time{})
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	for i, row := range filteredMetrics {
		s.Truef(strings.EqualFold(row.Database, targetDB) && strings.EqualFold(row.Schema, targetSchema),
			"QueryTableMetrics row[%d]: expected %s.%s, got %s.%s", i, targetDB, targetSchema, row.Database, row.Schema)
	}

	// Check QuerySqlDefinitions
	filteredDefs, err := s.Scrapper.QuerySqlDefinitions(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	for i, row := range filteredDefs {
		dbMatch := row.Database == "" || strings.EqualFold(row.Database, targetDB)
		s.Truef(dbMatch && strings.EqualFold(row.Schema, targetSchema),
			"QuerySqlDefinitions row[%d]: expected %s.%s, got %s.%s", i, targetDB, targetSchema, row.Database, row.Schema)
	}

	// Check QueryTableConstraints
	filteredConstraints, err := s.Scrapper.QueryTableConstraints(scopedCtx)
	if !isAcceptableError(err) {
		s.Require().NoError(err)
	}
	for i, row := range filteredConstraints {
		dbMatch := row.Database == "" || strings.EqualFold(row.Database, targetDB)
		s.Truef(dbMatch && strings.EqualFold(row.Schema, targetSchema),
			"QueryTableConstraints row[%d]: expected %s.%s, got %s.%s", i, targetDB, targetSchema, row.Database, row.Schema)
	}
}
