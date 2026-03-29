package scrappertest

import (
	"context"
	"errors"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// MonitorComplianceConfig provides the SQL queries and expected values for
// monitor compliance testing. Each warehouse test provides dialect-specific
// SQL and expectations.
type MonitorComplianceConfig struct {
	// SegmentsSQL is a query that returns a "segment" column.
	SegmentsSQL string

	// CustomMetricsSQL is a query returning segment_name and numeric columns, grouped by segment.
	CustomMetricsSQL string

	// ShapeSQL is a SELECT query with known columns.
	ShapeSQL string

	// ExpectedSegments are the segment values SegmentsSQL should return.
	// If empty, only checks that results are non-empty.
	ExpectedSegments []string

	// ExpectedShapeColumns are the ordered column names ShapeSQL should return.
	// Compared case-insensitively. If empty, only checks structural properties.
	ExpectedShapeColumns []string
}

// MonitorComplianceSuite validates that QuerySegments, QueryCustomMetrics, and
// QueryShape work correctly for a scrapper implementation.
//
// Embed this in warehouse-specific integration test suites alongside ComplianceSuite.
// Set Scrapper and Config before tests run (e.g., in SetupSuite).
type MonitorComplianceSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
	Config   MonitorComplianceConfig
}

// Ctx returns a background context for use in SetupSuite of embedding suites.
func (s *MonitorComplianceSuite) Ctx() context.Context {
	return context.Background()
}

func (s *MonitorComplianceSuite) ctx() context.Context {
	return context.Background()
}

func (s *MonitorComplianceSuite) TestMonitorCompliance_QuerySegments() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	if s.Config.SegmentsSQL == "" {
		s.T().Skip("SegmentsSQL not configured")
	}

	segments, err := s.Scrapper.QuerySegments(s.ctx(), s.Config.SegmentsSQL)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("QuerySegments unsupported")
	}
	s.Require().NoError(err)
	s.NotEmpty(segments, "QuerySegments should return results")

	for i, seg := range segments {
		s.NotEmptyf(seg.Segment, "QuerySegments row[%d].Segment should not be empty", i)
	}

	if len(s.Config.ExpectedSegments) > 0 {
		names := make([]string, len(segments))
		for i, seg := range segments {
			names[i] = seg.Segment
		}
		for _, expected := range s.Config.ExpectedSegments {
			s.Contains(names, expected, "Expected segment %q not found in results", expected)
		}
	}
}

func (s *MonitorComplianceSuite) TestMonitorCompliance_QueryCustomMetrics() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	if s.Config.CustomMetricsSQL == "" {
		s.T().Skip("CustomMetricsSQL not configured")
	}

	result, err := s.Scrapper.QueryCustomMetrics(s.ctx(), s.Config.CustomMetricsSQL)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("QueryCustomMetrics unsupported")
	}
	s.Require().NoError(err)
	s.NotEmpty(result, "QueryCustomMetrics should return results")

	for i, row := range result {
		s.NotEmptyf(row.Segments, "QueryCustomMetrics row[%d].Segments should not be empty", i)
		for j, seg := range row.Segments {
			s.NotEmptyf(seg.Name, "QueryCustomMetrics row[%d].Segments[%d].Name should not be empty", i, j)
		}

		s.NotEmptyf(row.ColumnValues, "QueryCustomMetrics row[%d].ColumnValues should not be empty", i)
		for j, col := range row.ColumnValues {
			s.NotEmptyf(col.Name, "QueryCustomMetrics row[%d].ColumnValues[%d].Name should not be empty", i, j)
		}
	}
}

func (s *MonitorComplianceSuite) TestMonitorCompliance_QueryShape() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	if s.Config.ShapeSQL == "" {
		s.T().Skip("ShapeSQL not configured")
	}

	columns, err := s.Scrapper.QueryShape(s.ctx(), s.Config.ShapeSQL)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("QueryShape unsupported")
	}
	s.Require().NoError(err)
	s.NotEmpty(columns, "QueryShape should return columns")

	for i, col := range columns {
		s.NotEmptyf(col.Name, "QueryShape column[%d].Name should not be empty", i)
		s.Equalf(int32(i+1), col.Position, "QueryShape column[%d].Position should be %d", i, i+1)
	}

	if len(s.Config.ExpectedShapeColumns) > 0 {
		s.Require().Len(columns, len(s.Config.ExpectedShapeColumns),
			"QueryShape should return %d columns", len(s.Config.ExpectedShapeColumns))
		for i, expected := range s.Config.ExpectedShapeColumns {
			s.Truef(strings.EqualFold(expected, columns[i].Name),
				"QueryShape column[%d]: expected %q, got %q (case-insensitive)", i, expected, columns[i].Name)
		}
	}
}
