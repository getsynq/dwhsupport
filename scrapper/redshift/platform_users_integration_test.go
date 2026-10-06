package redshift

import (
	"context"
	"os"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestRolesByLoginTrimsCharPadding(t *testing.T) {
	roles := rolesByLogin([]*platformUserRoleRow{
		{Login: "synq_user   ", Role: "sys:monitor "},
		{Login: "synq_user", Role: "loaders"},
		{Login: "IAM:jdoe", Role: "reporters"},
	})
	assert.Equal(t, map[string][]string{
		"synq_user": {"sys:monitor", "loaders"},
		"IAM:jdoe":  {"reporters"},
	}, roles)
}

// RedshiftPlatformUsersSuite lists the cluster's users. The dwhtesting user
// holds sys:monitor, which carries ACCESS SYSTEM TABLE, so it reads every fact.
type RedshiftPlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
	redshift *RedshiftScrapper
}

func TestRedshiftPlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift platform users tests in CI")
	}
	suite.Run(t, new(RedshiftPlatformUsersSuite))
}

func (s *RedshiftPlatformUsersSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)
	sc, err := NewRedshiftScrapper(context.Background(), &RedshiftScrapperConf{
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
	s.redshift = sc
	s.Scrapper = sc
	s.ConnectedLogin = testenv.EnvOrDefault("REDSHIFT_USER", "")
	s.ExpectFacts = []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactPlatformId,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactRoles,
		scrapper.PlatformUserFactLastLoginAt,
	}
}

func (s *RedshiftPlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// TestPlatformUsers_SeesEveryUser checks that the listing holds users other
// than the connected one, which PG_USER shows to every user.
func (s *RedshiftPlatformUsersSuite) TestPlatformUsers_SeesEveryUser() {
	users, err := scrappertest.OnlyPlatformUserSource(s.Scrapper.QueryPlatformUsers(context.Background()))
	s.Require().NoError(err)
	s.Equal(scrapper.PlatformUsersComplete, users.Completeness)
	s.Greater(len(users.Users), 1)
	for _, u := range users.Users {
		s.NotContainsf(u.Login, " ", "login %q is padded", u.Login)
	}
}

// TestPlatformUsers_WithoutAccessSystemTable runs the role and last-login
// steps as a user without ACCESS SYSTEM TABLE would: role grants and last
// logins are reported skipped rather than read for the connecting user alone,
// and group membership is still read.
func (s *RedshiftPlatformUsersSuite) TestPlatformUsers_WithoutAccessSystemTable() {
	ctx := context.Background()
	users := &scrapper.PlatformUserListing{Users: []*scrapper.PlatformUser{{Login: s.ConnectedLogin}}}
	s.Require().NoError(s.redshift.addPlatformUserRoles(ctx, users, false, nil))
	s.Require().NoError(s.redshift.addPlatformUserLastLogins(ctx, users, false, nil))

	s.True(users.IsSkipped(scrapper.PlatformUserFactRoles))
	s.True(users.IsSkipped(scrapper.PlatformUserFactLastLoginAt))
	s.Nil(users.Users[0].LastLoginAt)
	for _, role := range users.Users[0].Roles {
		s.NotEqual("sys:monitor", role, "an RBAC role grant was read without ACCESS SYSTEM TABLE")
	}
}
