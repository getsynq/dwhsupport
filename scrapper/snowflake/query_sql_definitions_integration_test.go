package snowflake

import (
	"context"
	"os"
	"strings"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

// SchemaDdlIntegrationSuite asks a real Snowflake account for the DDL of a
// schema by the name the account itself reports for it. The schema names a
// scan reads out of information_schema are exact, case included, so a GET_DDL
// has to address exactly that schema: a lower-case name must not resolve to an
// upper-case schema of the same letters. Snowflake folds an unquoted name to
// upper case, which is how a lower-case schema read as "does not exist or not
// authorized" on every scan, and how a lower-case schema with an upper-case
// twin was given the twin's DDL.
//
// It needs no privilege beyond reading the configured schema, which is why it
// probes with a case variant of an existing schema instead of creating one.
type SchemaDdlIntegrationSuite struct {
	suite.Suite
	ctx      context.Context
	scrapper *SnowflakeScrapper
	database string
	schema   string
}

func TestSchemaDdlIntegrationSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake integration tests in CI")
	}
	suite.Run(t, new(SchemaDdlIntegrationSuite))
}

func (s *SchemaDdlIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()
	s.database = os.Getenv("SNOWFLAKE_DATABASE")
	if s.database == "" {
		s.T().Skip("SNOWFLAKE_DATABASE env var not set")
	}
	s.schema = os.Getenv("SNOWFLAKE_SCHEMA")
	if s.schema == "" {
		s.schema = "PUBLIC"
	}
	if strings.ToLower(s.schema) == s.schema {
		s.T().Skipf("SNOWFLAKE_SCHEMA %q has no upper-case letter to probe a case variant with", s.schema)
	}

	sfConf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{s.database},
		Role:           os.Getenv("SNOWFLAKE_ROLE"),
		PrivateKeyFile: os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
	}
	if pk := os.Getenv("SNOWFLAKE_PRIVATE_KEY"); pk != "" {
		sfConf.PrivateKey = []byte(pk)
	}

	sc, err := NewSnowflakeScrapper(s.ctx, &SnowflakeScrapperConf{SnowflakeConf: sfConf})
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
	s.scrapper = sc
}

func (s *SchemaDdlIntegrationSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *SchemaDdlIntegrationSuite) TestSchemaDdlOfTheReportedName() {
	ddl, err := s.scrapper.getDdl(s.ctx, "SCHEMA", s.database, s.schema)
	s.Require().NoError(err)
	s.Contains(strings.ToUpper(ddl), "CREATE")
}

func (s *SchemaDdlIntegrationSuite) TestSchemaDdlOfACaseVariantNamesNoSchema() {
	// No schema is called the lower-case spelling of the configured one, so
	// the only correct answer is Snowflake's "does not exist or not
	// authorized". Getting the configured schema's DDL back instead means the
	// name was folded, and a real lower-case schema could never be reached.
	_, err := s.scrapper.getDdl(s.ctx, "SCHEMA", s.database, strings.ToLower(s.schema))
	s.Require().Errorf(err, "GET_DDL resolved %q to the schema %q", strings.ToLower(s.schema), s.schema)
	var sfErr *gosnowflake.SnowflakeError
	s.Require().True(errors.As(err, &sfErr), "not a Snowflake error: %v", err)
	s.Equal(gosnowflake.ErrObjectNotExistOrAuthorized, sfErr.Number)
}
