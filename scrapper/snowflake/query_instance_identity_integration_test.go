package snowflake

import (
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// SnowflakeInstanceIdentitySuite exercises QueryInstanceIdentity against a real
// account. The values it asserts on are the ones a caller derives account
// prefixes from, and the exact casing and hostname shapes are the part that
// must be observed rather than assumed — so re-run this against a customer-like
// account before trusting a derivation built on top of it.
//
// Configured through the same env vars as the compliance suite:
//
//	SNOWFLAKE_ACCOUNT / _USER / _PASSWORD (or _PRIVATE_KEY[_FILE]) / _WAREHOUSE / _ROLE / _DATABASE
type SnowflakeInstanceIdentitySuite struct {
	suite.Suite
	scrapper *SnowflakeScrapper
}

func TestSnowflakeInstanceIdentitySuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake integration tests in CI")
	}
	suite.Run(t, new(SnowflakeInstanceIdentitySuite))
}

func (s *SnowflakeInstanceIdentitySuite) SetupSuite() {
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if database == "" {
		s.T().Skip("SNOWFLAKE_DATABASE env var not set")
	}

	sfConf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{database},
		Role:           os.Getenv("SNOWFLAKE_ROLE"),
		PrivateKeyFile: os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
	}
	if pk := os.Getenv("SNOWFLAKE_PRIVATE_KEY"); pk != "" {
		sfConf.PrivateKey = []byte(pk)
	}

	sc, err := NewSnowflakeScrapper(s.T().Context(), &SnowflakeScrapperConf{SnowflakeConf: sfConf})
	require.NoError(s.T(), err)
	s.scrapper = sc
}

func (s *SnowflakeInstanceIdentitySuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *SnowflakeInstanceIdentitySuite) TestQueryInstanceIdentity() {
	identity, err := s.scrapper.QueryInstanceIdentity(s.T().Context())
	s.Require().NoError(err)
	s.Require().NotNil(identity)

	s.T().Logf("raw: %v", identity.Raw)
	for _, host := range identity.Hosts {
		s.T().Logf("host: %s (%s)", host.Host, host.Kind)
	}

	// The account locator is the one value every account reports, and the form
	// that a BI tool most often names the warehouse by.
	s.NotEmpty(identity.Get(scrapper.InstanceIdentityAccountLocator))

	// Both hostnames matter: the regionless one carries the
	// organization-qualified name, the regional one carries the locator plus a
	// region fragment that CURRENT_REGION does not spell the same way.
	kinds := map[string]string{}
	for _, host := range identity.Hosts {
		kinds[host.Kind] = host.Host
	}
	s.Contains(kinds, scrapper.InstanceHostRegional)
	s.Contains(kinds, scrapper.InstanceHostRegionless)

	// SYSTEM$ALLOWLIST reports only the account's own URLs here — anything with
	// a per-feature subdomain would break a caller treating a host as an
	// account identifier.
	for _, host := range identity.Hosts {
		s.Contains(host.Host, ".snowflakecomputing.com")
	}
}
