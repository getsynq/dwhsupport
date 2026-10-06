package db2

import (
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

type Db2PlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestDb2PlatformUsersSuite(t *testing.T) {
	skipInCI(t)
	suite.Run(t, new(Db2PlatformUsersSuite))
}

func (s *Db2PlatformUsersSuite) SetupSuite() {
	sc, err := newDb2ScrapperFromEnv(s.T().Context())
	if err != nil {
		s.T().Skipf("Could not connect to Db2: %v", err)
	}
	s.Scrapper = sc
	// Db2 folds an authorization ID to upper case.
	s.ConnectedLogin = strings.ToUpper(testenv.EnvOrDefault("DB2_USER", "synq_reader"))
}

func (s *Db2PlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func (s *Db2PlatformUsersSuite) db2() *Db2Scrapper {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	return s.Scrapper.(*Db2Scrapper)
}

func (s *Db2PlatformUsersSuite) TestDb2ListsGranteesWithRoles() {
	users, err := s.db2().QueryPlatformUsers(s.T().Context())
	s.Require().NoError(err)

	s.Equal(scrapper.PlatformUsersUnknown, users.Completeness, "Db2 keeps no list of its users")
	s.NotEmpty(users.CompletenessReason)
	s.False(users.IsSkipped(scrapper.PlatformUserFactRoles))
	s.True(users.IsSkipped(scrapper.PlatformUserFactEmail))

	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}
	s.NotContains(byLogin, "PUBLIC", "PUBLIC is a group, not a user")
	s.NotContains(byLogin, "SYSTS_ADM", "a role is not a user")
	// The instance owner created the database and holds DBADM; the seed grants it the text search roles.
	s.Require().Contains(byLogin, "DB2INST1")
	s.Contains(byLogin["DB2INST1"].Roles, "SYSTS_ADM")
	for login := range byLogin {
		s.Equal(strings.TrimSpace(login), login, "CHAR grantee columns are trimmed")
	}
}

// TestDb2CatalogFallbackListsTheSameUsers reads the SYSCAT union a role falls
// back to when SYSIBMADM.AUTHORIZATIONIDS is refused, and checks it lists what
// the admin view does.
func (s *Db2PlatformUsersSuite) TestDb2CatalogFallbackListsTheSameUsers() {
	sc := s.db2()
	full, err := sc.QueryPlatformUsers(s.T().Context())
	s.Require().NoError(err)
	fallback, err := sc.queryPlatformUsers(s.T().Context(), authIdSources[1:])
	s.Require().NoError(err)

	logins := func(users *scrapper.PlatformUsers) []string {
		var out []string
		for _, u := range users.Users {
			out = append(out, u.Login)
		}
		return out
	}
	s.Equal(logins(full), logins(fallback))
}

func (s *Db2PlatformUsersSuite) TestDb2ListsTheSessionUserEvenWithoutAGrant() {
	users, err := s.db2().queryPlatformUsers(s.T().Context(), nil)
	s.Require().NoError(err)
	s.Require().Len(users.Users, 1)
	s.Equal(s.ConnectedLogin, users.Users[0].Login)
}
