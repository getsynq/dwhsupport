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
	result, err := s.db2().QueryPlatformUsers(s.T().Context())
	s.Require().NoError(err)
	s.Require().Len(result.Sources, 1, "the admin view was read, so the SYSCAT union, a subset of it, is not")
	users := result.Source("db2.sysibmadm.authorizationids")
	s.Require().NotNil(users)
	s.Equal(scrapper.PlatformUserSourceSQL, users.Kind)
	s.Empty(users.Refused)

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

// refusedQuery is a read the test role may not run: MON_GET_CONNECTION needs
// EXECUTE, which PUBLIC does not hold, so Db2 answers SQLCODE -551. A view that
// does not exist would not do, since Db2 answers that with -204, not a refusal.
const refusedQuery = `SELECT TRIM(system_auth_id) AS authid FROM TABLE(MON_GET_CONNECTION(NULL, -2))`

// TestDb2FallsBackToTheCatalog refuses the admin view's source and checks that
// the SYSCAT union is read in its place and lists what the admin view does.
func (s *Db2PlatformUsersSuite) TestDb2FallsBackToTheCatalog() {
	sc := s.db2()
	refusedView := authIdSource{
		source: authIdSources[0].source,
		object: "MON_GET_CONNECTION",
		query:  refusedQuery,
	}
	_, err := sc.selectAuthIds(s.T().Context(), refusedView)
	if !sc.IsPermissionError(err) {
		s.T().Skipf("the test role may run MON_GET_CONNECTION, so it cannot stand in for a refusal: %v", err)
	}

	result, err := sc.queryPlatformUsers(s.T().Context(), []authIdSource{refusedView, authIdSources[1]})
	s.Require().NoError(err, "a refused admin view falls back rather than failing")
	s.False(result.AllRefused())
	s.Require().Len(result.Sources, 2)
	s.Equal("db2.sysibmadm.authorizationids", result.Sources[0].Source)
	s.NotEmpty(result.Sources[0].Refused)
	s.Empty(result.Sources[0].Users)
	fallback := result.Sources[1]
	s.Equal("db2.syscat_auth", fallback.Source)
	s.Empty(fallback.Refused)

	full, err := sc.QueryPlatformUsers(s.T().Context())
	s.Require().NoError(err)
	s.Equal(logins(full.Source("db2.sysibmadm.authorizationids")), logins(fallback))
}

func (s *Db2PlatformUsersSuite) TestDb2EverySourceRefusedIsAPermissionError() {
	sc := s.db2()
	refused := authIdSource{source: "db2.refused", object: "MON_GET_CONNECTION", query: refusedQuery}
	_, err := sc.selectAuthIds(s.T().Context(), refused)
	if !sc.IsPermissionError(err) {
		s.T().Skipf("the test role may run MON_GET_CONNECTION: %v", err)
	}
	result, err := sc.queryPlatformUsers(s.T().Context(), []authIdSource{refused, refused})
	s.Nil(result)
	s.True(sc.IsPermissionError(err), "a refused listing is a permission error, not an empty list: %v", err)
}

func (s *Db2PlatformUsersSuite) TestDb2ListsTheSessionUserEvenWithoutAGrant() {
	nobody := authIdSource{
		source: "db2.nobody",
		object: "SYSIBMADM.AUTHORIZATIONIDS",
		query:  `SELECT TRIM(authid) AS authid FROM SYSIBMADM.AUTHORIZATIONIDS WHERE 1 = 0`,
	}
	result, err := s.db2().queryPlatformUsers(s.T().Context(), []authIdSource{nobody})
	s.Require().NoError(err)
	users := result.Source("db2.nobody")
	s.Require().NotNil(users)
	s.Require().Len(users.Users, 1)
	s.Equal(s.ConnectedLogin, users.Users[0].Login)
}

func logins(users *scrapper.PlatformUserListing) []string {
	var out []string
	for _, u := range users.Users {
		out = append(out, u.Login)
	}
	return out
}
