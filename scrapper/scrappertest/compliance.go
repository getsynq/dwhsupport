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
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

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
	return context.Background()
}

// isAcceptableError returns true if err is nil or ErrUnsupported.
func isAcceptableError(err error) bool {
	return err == nil || errors.Is(err, scrapper.ErrUnsupported)
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
	for i, row := range databases {
		s.NotEmptyf(row.Instance, "QueryDatabases row[%d].Instance", i)
		s.NotEmptyf(row.Database, "QueryDatabases row[%d].Database", i)
		allInstances = append(allInstances, row.Instance)
	}

	// --- QueryTables ---
	tables, err := s.Scrapper.QueryTables(ctx)
	if !s.True(isAcceptableError(err), "QueryTables error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range tables {
		s.NotEmptyf(row.Instance, "QueryTables row[%d].Instance", i)
		s.NotEmptyf(row.Database, "QueryTables row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryTables row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryTables row[%d].Table", i)
		allInstances = append(allInstances, row.Instance)
	}

	// --- QueryCatalog ---
	catalog, err := s.Scrapper.QueryCatalog(ctx)
	if !s.True(isAcceptableError(err), "QueryCatalog error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range catalog {
		s.NotEmptyf(row.Instance, "QueryCatalog row[%d].Instance", i)
		s.NotEmptyf(row.Database, "QueryCatalog row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryCatalog row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryCatalog row[%d].Table", i)
		s.NotEmptyf(row.Column, "QueryCatalog row[%d].Column", i)
		s.NotEmptyf(row.Type, "QueryCatalog row[%d].Type", i)
		allInstances = append(allInstances, row.Instance)
	}

	// --- QueryTableMetrics ---
	metrics, err := s.Scrapper.QueryTableMetrics(ctx, time.Time{})
	if !s.True(isAcceptableError(err), "QueryTableMetrics error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range metrics {
		s.NotEmptyf(row.Instance, "QueryTableMetrics row[%d].Instance", i)
		s.NotEmptyf(row.Database, "QueryTableMetrics row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryTableMetrics row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryTableMetrics row[%d].Table", i)
		allInstances = append(allInstances, row.Instance)
	}

	// --- QuerySqlDefinitions ---
	defs, err := s.Scrapper.QuerySqlDefinitions(ctx)
	if !s.True(isAcceptableError(err), "QuerySqlDefinitions error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range defs {
		s.NotEmptyf(row.Instance, "QuerySqlDefinitions row[%d].Instance", i)
		s.NotEmptyf(row.Database, "QuerySqlDefinitions row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QuerySqlDefinitions row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QuerySqlDefinitions row[%d].Table", i)
		s.NotEmptyf(row.Sql, "QuerySqlDefinitions row[%d].Sql", i)
		allInstances = append(allInstances, row.Instance)
	}

	// --- QueryTableConstraints ---
	constraints, err := s.Scrapper.QueryTableConstraints(ctx)
	if !s.True(isAcceptableError(err), "QueryTableConstraints error: %v", err) {
		s.T().FailNow()
	}
	for i, row := range constraints {
		s.NotEmptyf(row.Instance, "QueryTableConstraints row[%d].Instance", i)
		s.NotEmptyf(row.Database, "QueryTableConstraints row[%d].Database", i)
		s.NotEmptyf(row.Schema, "QueryTableConstraints row[%d].Schema", i)
		s.NotEmptyf(row.Table, "QueryTableConstraints row[%d].Table", i)
		s.NotEmptyf(row.ConstraintName, "QueryTableConstraints row[%d].ConstraintName", i)
		s.NotEmptyf(row.ConstraintType, "QueryTableConstraints row[%d].ConstraintType", i)
		s.NotEmptyf(row.ColumnName, "QueryTableConstraints row[%d].ColumnName", i)
		allInstances = append(allInstances, row.Instance)
	}

	// --- Cross-method consistency: all Instance values must be equal ---
	if len(allInstances) > 1 {
		first := allInstances[0]
		for i, inst := range allInstances[1:] {
			s.Equalf(first, inst, "Instance mismatch across scrapper methods at index %d: got %q, want %q", i+1, inst, first)
		}
	}
}
