package redshift

import (
	"os"
	"regexp"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// RedshiftInstanceIdentitySuite runs QueryInstanceIdentity against a real
// warehouse, because the namespace format is what a caller joins hostnames on.
type RedshiftInstanceIdentitySuite struct {
	suite.Suite
	scrapper *RedshiftScrapper
}

func TestRedshiftInstanceIdentitySuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift integration tests in CI")
	}
	suite.Run(t, new(RedshiftInstanceIdentitySuite))
}

func (s *RedshiftInstanceIdentitySuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)
	sc, err := NewRedshiftScrapper(s.T().Context(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.scrapper = sc
}

func (s *RedshiftInstanceIdentitySuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (s *RedshiftInstanceIdentitySuite) TestReportsTheNamespace() {
	identity, err := s.scrapper.QueryInstanceIdentity(s.T().Context())
	s.Require().NoError(err)
	s.Regexp(uuidPattern, identity.Get(scrapper.InstanceIdentityNamespace))
	s.Empty(identity.Hosts)
}

func (s *RedshiftInstanceIdentitySuite) TestIsReachableThroughAs() {
	querier, ok := scrapper.As[scrapper.InstanceIdentityQuerier](s.scrapper)
	s.Require().True(ok)
	identity, err := querier.QueryInstanceIdentity(s.T().Context())
	s.Require().NoError(err)
	s.NotEmpty(identity.Get(scrapper.InstanceIdentityNamespace))
}
