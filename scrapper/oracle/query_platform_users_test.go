package oracle

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestOracleAccountDisabled(t *testing.T) {
	for _, tc := range []struct {
		status, auth string
		disabled     bool
	}{
		{"OPEN", "PASSWORD", false},
		{"EXPIRED(GRACE)", "PASSWORD", false},
		{"IN ROLLOVER", "PASSWORD", false},
		{"EXPIRED", "PASSWORD", true},
		{"LOCKED", "PASSWORD", true},
		{"LOCKED(TIMED)", "PASSWORD", true},
		{"EXPIRED & LOCKED", "PASSWORD", true},
		{"EXPIRED(GRACE) & LOCKED(TIMED)", "PASSWORD", true},
		{"OPEN", "NONE", true},
		{"open", "EXTERNAL", false},
	} {
		assert.Equalf(t, tc.disabled, oracleAccountDisabled(tc.status, tc.auth), "%s / %s", tc.status, tc.auth)
	}
}

func TestOracleUserRowToPlatformUser(t *testing.T) {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	assert.NoError(t, err)
	row := &oracleUserRow{
		Username:           "SVC_LOADER",
		UserId:             sql.NullInt64{Int64: 142, Valid: true},
		AccountStatus:      sql.NullString{String: "LOCKED", Valid: true},
		AuthenticationType: sql.NullString{String: "PASSWORD", Valid: true},
		// A driver labelling the zone-less UTC value with the host zone must not shift it.
		CreatedUtc: sql.NullTime{Time: time.Date(2024, 3, 1, 10, 0, 0, 0, warsaw), Valid: true},
	}
	u := row.toPlatformUser()
	assert.Equal(t, "SVC_LOADER", u.Login)
	assert.Equal(t, "142", u.PlatformId)
	assert.Equal(t, "PASSWORD", u.Type)
	assert.True(t, *u.Disabled)
	assert.Equal(t, time.Date(2024, 3, 1, 10, 0, 0, 0, time.UTC), *u.CreatedAt)
	assert.Nil(t, u.LastLoginAt)

	// The ALL_USERS fallback has no status: disabled is unknown, not false.
	u = (&oracleUserRow{Username: "X"}).toPlatformUser()
	assert.Nil(t, u.Disabled)
	assert.Empty(t, u.PlatformId)
}

type OraclePlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestOraclePlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle tests in CI")
	}
	suite.Run(t, new(OraclePlatformUsersSuite))
}

func (s *OraclePlatformUsersSuite) SetupSuite() {
	sc, err := newOracleScrapperFromEnv(s.T().Context())
	if err != nil {
		s.T().Skipf("Could not connect to Oracle: %v", err)
	}
	s.Scrapper = sc
	s.ConnectedLogin = strings.ToUpper(testenv.EnvOrDefault("ORACLE_USER", "synq"))
	s.MatchLoginFold = true
	s.ExpectFacts = []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactPlatformId,
		scrapper.PlatformUserFactType,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactCreatedAt,
		scrapper.PlatformUserFactLastLoginAt,
	}
}

func (s *OraclePlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		s.Scrapper.Close()
	}
}

func (s *OraclePlatformUsersSuite) oracle() *OracleScrapper {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	return s.Scrapper.(*OracleScrapper)
}

func (s *OraclePlatformUsersSuite) TestOracleListsMaintainedUsersAndRoles() {
	users, err := s.oracle().QueryPlatformUsers(s.T().Context())
	s.Require().NoError(err)
	s.Equal(scrapper.PlatformUsersComplete, users.Completeness)

	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}
	s.Require().Contains(byLogin, "SYS", "Oracle-maintained users are logins and are listed")
	s.Require().Contains(byLogin, "SYSTEM")
	s.Contains(byLogin["SYSTEM"].Roles, "DBA")
	s.False(*byLogin["SYSTEM"].Disabled)
	if xs, ok := byLogin["XS$NULL"]; ok {
		s.True(*xs.Disabled, "XS$NULL is a locked schema-only account")
	}
	s.False(users.IsSkipped(scrapper.PlatformUserFactRoles))
	s.True(users.IsSkipped(scrapper.PlatformUserFactEmail))
}

// TestOracleFallsBackToAllUsers points the listing at views that do not exist.
// Oracle answers a view the role may not read with the same ORA-00942, so this
// is what a role without SELECT_CATALOG_ROLE gets.
func (s *OraclePlatformUsersSuite) TestOracleFallsBackToAllUsers() {
	sc := s.oracle()
	users, err := sc.queryPlatformUsers(s.T().Context(), platformUserViews{users: "DBA_USERS_REFUSED", roleGrant: "DBA_ROLE_PRIVS_REFUSED"})
	s.Require().NoError(err, "a refused DBA_USERS falls back rather than failing")

	s.Equal(scrapper.PlatformUsersComplete, users.Completeness, "ALL_USERS lists every user")
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactType, scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserFactRoles,
	} {
		s.Truef(users.IsSkipped(fact), "%s should be skipped", fact)
	}

	full, err := sc.QueryPlatformUsers(s.T().Context())
	s.Require().NoError(err)
	s.Equal(len(full.Users), len(users.Users), "ALL_USERS and DBA_USERS list the same users")
	for _, u := range users.Users {
		s.Nil(u.Disabled)
		s.Nil(u.Roles)
		s.NotEmpty(u.PlatformId)
		s.NotNil(u.CreatedAt)
	}
}
