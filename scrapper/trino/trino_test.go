package trino

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type ValidateConfigSuite struct {
	suite.Suite
}

func TestValidateConfigSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(ValidateConfigSuite))
}

func (s *ValidateConfigSuite) newScrapperWithCatalogs(catalogs []string) *TrinoScrapper {
	ctx := context.TODO()
	conf := &trino.TrinoConf{
		User:     os.Getenv("STARBURST_USER"),
		Password: os.Getenv("STARBURST_PASSWORD"),
		Host:     "synq-free-gcp.trino.galaxy.starburst.io",
		Port:     443,
	}
	scr, err := NewTrinoScrapper(ctx, &TrinoScrapperConf{
		TrinoConf:              conf,
		Catalogs:               catalogs,
		FetchMaterializedViews: false,
		FetchTableComments:     false,
	})
	s.Require().NoError(err)
	s.Require().NotNil(scr)
	return scr
}

func (s *ValidateConfigSuite) TestValidateConfiguration_AllCatalogsPresent() {
	scr := s.newScrapperWithCatalogs([]string{"iceberg_gcs"})
	defer scr.Close()
	ctx := context.TODO()
	msgs, err := scr.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	s.Empty(msgs)
}

func (s *ValidateConfigSuite) TestValidateConfiguration_MissingCatalogs() {
	scr := s.newScrapperWithCatalogs([]string{"iceberg_gcs", "definitely_missing_catalog"})
	defer scr.Close()
	ctx := context.TODO()
	msgs, err := scr.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	s.NotEmpty(msgs)
	s.Contains(msgs[0], "definitely_missing_catalog")
}

func (s *ValidateConfigSuite) TestValidateConfiguration_UnavailableCatalog() {
	scr := s.newScrapperWithCatalogs([]string{"iceberg_gcs", "kernel_ext_dbt_pg"})
	defer scr.Close()
	ctx := context.TODO()
	msgs, err := scr.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	s.NotEmpty(msgs, "Should have warnings about unavailable catalog")

	// Check that we got a warning about kernel_ext_dbt_pg being unavailable
	foundWarning := false
	for _, msg := range msgs {
		if s.Contains(msg, "kernel_ext_dbt_pg") && s.Contains(msg, "no longer available") {
			foundWarning = true
			break
		}
	}
	s.True(foundWarning, "Should warn about kernel_ext_dbt_pg being unavailable")
}

func (s *ValidateConfigSuite) TestQueryTableMetrics_WithUnavailableCatalog() {
	scr := s.newScrapperWithCatalogs([]string{"iceberg_gcs", "tpch", "kernel_ext_dbt_pg"})
	defer scr.Close()
	ctx := context.TODO()

	// First validate to see warnings
	msgs, err := scr.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	if len(msgs) > 0 {
		s.T().Logf("Validation warnings: %v", msgs)
	}

	// Try to fetch metrics - should succeed even with unavailable catalog
	metrics, err := scr.QueryTableMetrics(ctx, time.Time{})
	s.Require().NoError(err, "QueryTableMetrics should succeed despite unavailable catalog")
	s.NotNil(metrics)

	// We should have metrics from working catalogs (iceberg_gcs, tpch)
	// but not fail due to kernel_ext_dbt_pg
	s.T().Logf("Successfully fetched %d table metrics", len(metrics))
}

type ErrorDetectionSuite struct {
	suite.Suite
}

func TestErrorDetectionSuite(t *testing.T) {
	suite.Run(t, new(ErrorDetectionSuite))
}

func (s *ErrorDetectionSuite) TestIsCatalogUnavailableError() {
	testCases := []struct {
		name     string
		errMsg   string
		expected bool
	}{
		{
			name:     "catalog not found",
			errMsg:   "line 1:15: catalog 'missing_catalog' not found",
			expected: true,
		},
		{
			name:     "catalog not registered",
			errMsg:   "line 1:15: catalog 'xyz' not registered",
			expected: true,
		},
		{
			name:     "catalog does not exist",
			errMsg:   "catalog 'test' does not exist",
			expected: true,
		},
		{
			name:     "catalog connection failed",
			errMsg:   "EXTERNAL: Error listing tables for catalog kernel_ext_dbt_pg: The connection attempt failed.",
			expected: true,
		},
		{
			name:     "catalog with failed keyword",
			errMsg:   "trino: query failed (200 OK): \"EXTERNAL: Error listing tables for catalog kernel_ext_dbt_pg: The connection attempt failed.\"",
			expected: true,
		},
		{
			name:     "catalog unreachable",
			errMsg:   "catalog 'remote_catalog' unreachable",
			expected: true,
		},
		{
			name:     "catalog unavailable",
			errMsg:   "catalog 'shared_catalog' unavailable",
			expected: true,
		},
		{
			name:     "other error",
			errMsg:   "permission denied",
			expected: false,
		},
		{
			name:     "syntax error",
			errMsg:   "line 1:15: mismatched input 'FROM'",
			expected: false,
		},
		{
			name:     "nil error",
			errMsg:   "",
			expected: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			var err error
			if tc.errMsg != "" {
				err = fmt.Errorf("%s", tc.errMsg)
			}
			result := isCatalogUnavailableError(err)
			s.Equal(tc.expected, result, "Error message: %s", tc.errMsg)
		})
	}
}

func (s *ErrorDetectionSuite) TestIsCatalogUnavailableError_CaseInsensitive() {
	testCases := []string{
		"CATALOG 'TEST' NOT FOUND",
		"Catalog 'test' Not Found",
		"CATALOG 'TEST' DOES NOT EXIST",
		"CATALOG CONNECTION FAILED",
	}

	for _, errMsg := range testCases {
		s.Run(errMsg, func() {
			err := fmt.Errorf("%s", errMsg)
			result := isCatalogUnavailableError(err)
			s.True(result, "Should detect error: %s", errMsg)
		})
	}
}
