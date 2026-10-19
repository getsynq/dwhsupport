package trino

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	trinoclient "github.com/trinodb/trino-go-client/trino"
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

// queryFailed is an error the way trino-go-client returns one Trino reported.
func queryFailed(errorName, errorType, message string) error {
	return &trinoclient.ErrQueryFailed{
		StatusCode: 200,
		Reason:     &trinoclient.ErrTrino{ErrorName: errorName, ErrorType: errorType, Message: message},
	}
}

func (s *ErrorDetectionSuite) TestIsCatalogUnavailableError() {
	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "catalog not found",
			err:      queryFailed("CATALOG_NOT_FOUND", "USER_ERROR", "line 1:15: Catalog 'missing_catalog' not found"),
			expected: true,
		},
		{
			name:     "catalog failed to load",
			err:      queryFailed("CATALOG_UNAVAILABLE", "USER_ERROR", "Catalog 'broken' failed to initialize and is disabled"),
			expected: true,
		},
		{
			name:     "catalog not available on a worker",
			err:      queryFailed("CATALOG_NOT_AVAILABLE", "INTERNAL_ERROR", "No catalog 'broken'"),
			expected: true,
		},
		{
			name:     "JDBC catalog whose database is down",
			err:      queryFailed("JDBC_ERROR", "EXTERNAL", "The connection attempt failed."),
			expected: true,
		},
		{
			name: "JDBC catalog whose database is down, while listing",
			err: queryFailed("JDBC_ERROR", "EXTERNAL",
				"Error listing tables for catalog kernel_ext_dbt_pg: The connection attempt failed."),
			expected: true,
		},
		{
			name:     "wrapped",
			err:      errors.Wrap(queryFailed("CATALOG_NOT_FOUND", "USER_ERROR", "Catalog 'x' not found"), "query failed"),
			expected: true,
		},
		{
			name: "columns of a catalog whose storage cannot be read",
			err: queryFailed("ICEBERG_FILESYSTEM_ERROR", "EXTERNAL",
				"Error listing table columns for catalog iceberg: Failed to open input stream for file: s3://lake/metadata.json"),
			expected: false,
		},
		{
			name: "tables of a catalog whose storage cannot be read",
			err: queryFailed("GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR",
				"Error listing tables for catalog iceberg: Failed to open input stream for file: s3://lake/metadata.json"),
			expected: false,
		},
		{
			name: "a table that does not exist",
			err: queryFailed("TABLE_NOT_FOUND", "USER_ERROR",
				"line 1:15: Table 'iceberg.information_schema.table_constraints' does not exist"),
			expected: false,
		},
		{
			name:     "connection failure outside the connector",
			err:      queryFailed("GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR", "The connection attempt failed."),
			expected: false,
		},
		{
			name:     "not a Trino error, even when its text says so",
			err:      errors.New("catalog 'x' not found"),
			expected: false,
		},
		{
			name:     "permission denied",
			err:      queryFailed("PERMISSION_DENIED", "USER_ERROR", "Access Denied: Cannot select from columns"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.Equal(tc.expected, isCatalogUnavailableError(tc.err), "error: %v", tc.err)
		})
	}
}
