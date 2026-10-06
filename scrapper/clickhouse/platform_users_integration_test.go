package clickhouse

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// ClickHousePlatformUsersSuite runs the shared user listing checks, and then
// checks what a user with fewer grants gets, with throwaway users and a role it
// creates and drops.
type ClickHousePlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
	admin *ClickhouseScrapper
	// suffix keeps the throwaway users of concurrent runs apart.
	suffix string
}

func TestClickHousePlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse platform users tests in CI")
	}
	suite.Run(t, new(ClickHousePlatformUsersSuite))
}

func (s *ClickHousePlatformUsersSuite) SetupSuite() {
	if testenv.EnvOrDefault("CLICKHOUSE_HOST", "") == "" {
		s.T().Skip("CLICKHOUSE_HOST env var not set")
	}
	sc, err := newClickhouseScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to ClickHouse: %v", err)
	}
	s.admin = sc
	s.Scrapper = sc
	s.ConnectedLogin = os.Getenv("CLICKHOUSE_USER")
	s.ExpectFacts = []scrapper.PlatformUserFact{scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserFactDisabled}
	s.suffix = fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
}

func (s *ClickHousePlatformUsersSuite) TearDownSuite() {
	if s.admin != nil {
		_ = s.admin.Close()
	}
}

const throwawayPassword = "Pu_test_pw_9871"

// createUser creates a user, dropped at the end of the test. grants are run as
// the admin after creation, with %s standing for the user.
func (s *ClickHousePlatformUsersSuite) createUser(name string, ddlSuffix string, grants ...string) {
	ctx := context.Background()
	exec := s.admin.executor
	s.Require().NoError(exec.Exec(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY '%s' %s", name, throwawayPassword, ddlSuffix)))
	s.T().Cleanup(func() { _ = exec.Exec(context.Background(), "DROP USER IF EXISTS "+name) })
	for _, g := range grants {
		s.Require().NoError(exec.Exec(ctx, fmt.Sprintf(g, name)))
	}
}

// throwawayUser creates a user like createUser and returns a scrapper connected
// as it.
func (s *ClickHousePlatformUsersSuite) throwawayUser(name string, ddlSuffix string, grants ...string) *ClickhouseScrapper {
	ctx := context.Background()
	s.createUser(name, ddlSuffix, grants...)
	sc, err := NewClickhouseScrapper(ctx, ClickhouseScrapperConf{
		ClickhouseConf: dwhexecclickhouse.ClickhouseConf{
			Hostname:        os.Getenv("CLICKHOUSE_HOST"),
			Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
			Username:        name,
			Password:        throwawayPassword,
			DefaultDatabase: "default",
			NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
		},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = sc.Close() })
	return sc
}

func (s *ClickHousePlatformUsersSuite) findUser(users *scrapper.PlatformUserListing, login string) *scrapper.PlatformUser {
	for _, u := range users.Users {
		if u.Login == login {
			return u
		}
	}
	return nil
}

func (s *ClickHousePlatformUsersSuite) TestPlatformUsers_RolesDefaultRoleAndExpiry() {
	ctx := context.Background()
	exec := s.admin.executor
	role := "pu_role_" + s.suffix
	other := "pu_other_" + s.suffix
	s.Require().NoError(exec.Exec(ctx, "CREATE ROLE "+role))
	s.T().Cleanup(func() { _ = exec.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })
	s.Require().NoError(exec.Exec(ctx, "CREATE ROLE "+other))
	s.T().Cleanup(func() { _ = exec.Exec(context.Background(), "DROP ROLE IF EXISTS "+other) })

	withRoles := "pu_roles_" + s.suffix
	s.createUser(withRoles, "", "GRANT "+role+", "+other+" TO %s", "SET DEFAULT ROLE "+role+" TO %s")
	expired := "pu_expired_" + s.suffix
	s.createUser(expired, "VALID UNTIL '2001-01-01 00:00:00'")

	users, err := scrappertest.OnlyPlatformUserSource(s.admin.QueryPlatformUsers(ctx))
	s.Require().NoError(err)
	s.Equal(scrapper.PlatformUsersComplete, users.Completeness)
	s.False(users.IsSkipped(scrapper.PlatformUserFactRoles))
	for _, f := range users.SkippedFacts {
		s.NotContainsf(f.Reason, "failed:", "the admin may read everything; %s was skipped for a reason that is not the platform's", f.Fact)
	}

	u := s.findUser(users, withRoles)
	s.Require().NotNil(u)
	s.Equal([]string{other, role}, u.Roles)
	s.Equal(role, u.DefaultRole)
	s.Require().NotNil(u.Disabled)
	s.False(*u.Disabled)
	s.NotEmpty(u.PlatformId)

	e := s.findUser(users, expired)
	s.Require().NotNil(e)
	s.Require().NotNil(e.Disabled)
	s.True(*e.Disabled, "a user whose only password expired cannot sign in")
	s.Empty(e.Roles)
	s.Empty(e.DefaultRole)
}

func (s *ClickHousePlatformUsersSuite) TestPlatformUsers_NoGrantIsARefusedSource() {
	sc := s.throwawayUser("pu_nogrant_"+s.suffix, "")

	result, err := sc.QueryPlatformUsers(context.Background())
	s.Require().NoError(err, "a refused system.users is a state of the result, not an error of the call")
	src, err := scrappertest.OnlyPlatformUserSource(result, nil)
	s.Require().NoError(err)
	s.Equal(platformUsersSource, src.Source)
	s.Contains(src.Refused, "system.users", "the fan-out falls back to the local read, whose refusal names system.users")
	s.Empty(src.Unavailable)
	s.Empty(src.Failed)
	s.Empty(src.Users)
	s.NotEmpty(result.Reconcile().Refused)
}

func (s *ClickHousePlatformUsersSuite) TestPlatformUsers_NoRemoteReadsTheNodeAlone() {
	login := "pu_noremote_" + s.suffix
	sc := s.throwawayUser(login, "", "GRANT SELECT ON system.users TO %s", "GRANT SELECT ON system.role_grants TO %s")

	users, err := scrappertest.OnlyPlatformUserSource(sc.QueryPlatformUsers(context.Background()))
	s.Require().NoError(err)
	s.True(users.Answered())
	s.Equal(scrapper.PlatformUsersLimited, users.Completeness)
	s.Contains(users.CompletenessReason, "READ ON REMOTE")
	s.NotNil(s.findUser(users, login))
	s.NotNil(s.findUser(users, os.Getenv("CLICKHOUSE_USER")))
	s.False(users.IsSkipped(scrapper.PlatformUserFactRoles), "role grants fall back to the local read too: %v", users.SkippedFacts)
}

func (s *ClickHousePlatformUsersSuite) TestPlatformUsers_RefusedRolesAreSkipped() {
	login := "pu_noroles_" + s.suffix
	sc := s.throwawayUser(login, "", "GRANT SELECT ON system.users TO %s", "GRANT READ ON REMOTE TO %s")

	users, err := scrappertest.OnlyPlatformUserSource(sc.QueryPlatformUsers(context.Background()))
	s.Require().NoError(err)
	s.Equal(scrapper.PlatformUsersComplete, users.Completeness)
	s.NotNil(s.findUser(users, login))
	s.NotNil(s.findUser(users, os.Getenv("CLICKHOUSE_USER")), "SELECT ON system.users lists every user, not only itself")
	s.True(users.IsSkipped(scrapper.PlatformUserFactRoles))
	s.True(users.IsSkipped(scrapper.PlatformUserFactDefaultRole))
	for _, f := range users.SkippedFacts {
		if f.Fact == scrapper.PlatformUserFactRoles {
			s.Contains(f.Reason, "system.role_grants")
		}
	}
}
