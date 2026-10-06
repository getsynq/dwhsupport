package mysql

import (
	"context"
	"os"
	"testing"

	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MySQLPlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestMySQLPlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL platform users tests in CI")
	}
	suite.Run(t, new(MySQLPlatformUsersSuite))
}

func (s *MySQLPlatformUsersSuite) SetupSuite() {
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	s.ConnectedLogin = testenv.EnvOrDefault("MYSQL_USER", "synq")
}

func (s *MySQLPlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type MariaDBPlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestMariaDBPlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB platform users tests in CI")
	}
	suite.Run(t, new(MariaDBPlatformUsersSuite))
}

func (s *MariaDBPlatformUsersSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
	s.ConnectedLogin = testenv.EnvOrDefault("MARIADB_USER", "synq")
	s.ExpectFacts = []scrapper.PlatformUserFact{scrapper.PlatformUserFactDisabled}
}

func (s *MariaDBPlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// TestMySQLPlatformUsers_WithoutMysqlSchema runs as the test login, which has
// no SELECT on the mysql schema: the listing falls back to
// information_schema, which shows only the login itself, and says it is
// limited rather than failing or claiming to be complete.
func TestMySQLPlatformUsers_WithoutMysqlSchema(t *testing.T) {
	if os.Getenv("CI") != "" || testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		t.Skip("MYSQL_HOST env var not set")
	}
	ctx := context.Background()
	sc, err := newMySQLScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to MySQL: %v", err)
	}
	defer sc.Close()

	_, err = sc.executor.GetDb().ExecContext(ctx, "SELECT 1 FROM mysql.user LIMIT 1")
	if err == nil {
		t.Skip("the test login may read mysql.user, so the limited path is not exercised")
	}
	require.True(t, sc.IsPermissionError(err), "reading mysql.user without the grant is a permission error: %v", err)

	result, err := sc.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	assertRefusedThenLimited(t, result, sourceMySQLUser, sourceMySQLUserAttributes, testenv.EnvOrDefault("MYSQL_USER", "synq"))
}

// assertRefusedThenLimited checks the shape of a listing by a login that may
// not read the mysql schema: the mysql schema source refused, then the
// information_schema source holding only the login itself.
func assertRefusedThenLimited(t *testing.T, result *scrapper.PlatformUsers, mainSource, fallbackSource, login string) {
	t.Helper()
	require.Len(t, result.Sources, 2)
	refused := result.Sources[0]
	assert.Equal(t, mainSource, refused.Source)
	assert.NotEmpty(t, refused.Refused)
	assert.Empty(t, refused.Users)

	limited := result.Sources[1]
	assert.Equal(t, fallbackSource, limited.Source)
	assert.Equal(t, scrapper.PlatformUserSourceSQL, limited.Kind)
	assert.Empty(t, limited.Refused)
	assert.Equal(t, scrapper.PlatformUsersLimited, limited.Completeness)
	assert.Contains(t, limited.CompletenessReason, "mysql")
	require.Len(t, limited.Users, 1)
	assert.Equal(t, login, limited.Users[0].Login)
	assert.True(t, limited.IsSkipped(scrapper.PlatformUserFactRoles))
	assert.True(t, limited.IsSkipped(scrapper.PlatformUserFactDisabled))

	reconciled := result.Reconcile()
	assert.Equal(t, scrapper.PlatformUsersLimited, reconciled.Completeness)
	assert.Contains(t, reconciled.CompletenessReason, mainSource+" refused")
}

// TestMariaDBPlatformUsers_RolesLocksAndLimitedLogin creates a role, a user
// holding it as its default, a locked user and a user with no privileges,
// then checks what the privileged test login and the unprivileged one see.
// The test login must be able to create users (root on dwhtesting).
func TestMariaDBPlatformUsers_RolesLocksAndLimitedLogin(t *testing.T) {
	if os.Getenv("CI") != "" || testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		t.Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(context.Background())
	if err != nil {
		t.Skipf("Could not connect to MariaDB: %v", err)
	}
	testRolesLocksAndLimitedLogin(t, sc, "MARIADB", sourceMariaDBGlobalPriv, sourceMariaDBUserPrivileges,
		"SET DEFAULT ROLE pu_reporter FOR 'pu_analyst'@'%'", "")
}

// TestMySQLPlatformUsers_RolesLocksAndLimitedLogin is the MySQL run of the
// same check. The dwhtesting test login may not create users, so it runs only
// when MYSQL_USER is overridden with one that may (root).
func TestMySQLPlatformUsers_RolesLocksAndLimitedLogin(t *testing.T) {
	if os.Getenv("CI") != "" || testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		t.Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(context.Background())
	if err != nil {
		t.Skipf("Could not connect to MySQL: %v", err)
	}
	testRolesLocksAndLimitedLogin(t, sc, "MYSQL", sourceMySQLUser, sourceMySQLUserAttributes,
		"SET DEFAULT ROLE pu_reporter TO 'pu_analyst'@'%'", " COMMENT 'runs the nightly load'")
}

func testRolesLocksAndLimitedLogin(t *testing.T, sc *MySQLScrapper, envPrefix, mainSource, fallbackSource, setDefaultRole, comment string) {
	ctx := context.Background()
	defer sc.Close()
	db := sc.executor.GetDb()

	const password = "PlatformUsers1!"
	setup := []string{
		"DROP USER IF EXISTS 'pu_analyst'@'%', 'pu_locked'@'%', 'pu_nopriv'@'%'",
		"DROP ROLE IF EXISTS pu_reporter",
		"CREATE ROLE pu_reporter",
		"CREATE USER 'pu_analyst'@'%' IDENTIFIED BY '" + password + "'" + comment,
		"GRANT pu_reporter TO 'pu_analyst'@'%'",
		setDefaultRole,
		"CREATE USER 'pu_locked'@'%' IDENTIFIED BY '" + password + "' ACCOUNT LOCK",
		"CREATE USER 'pu_nopriv'@'%' IDENTIFIED BY '" + password + "'",
	}
	for _, stmt := range setup {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Skipf("the test login may not manage users (%s): %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP USER IF EXISTS 'pu_analyst'@'%', 'pu_locked'@'%', 'pu_nopriv'@'%'")
		_, _ = db.ExecContext(context.Background(), "DROP ROLE IF EXISTS pu_reporter")
	})

	result, err := sc.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	require.Len(t, result.Sources, 1, "information_schema only repeats the mysql schema, so it is not read when that answered")
	users := result.Sources[0]
	assert.Equal(t, mainSource, users.Source)
	assert.Equal(t, scrapper.PlatformUsersComplete, users.Completeness)
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactDefaultRole))
	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}
	assert.NotContains(t, byLogin, "pu_reporter", "a role is not a login")
	assert.NotContains(t, byLogin, "mariadb.sys", "a reserved account is not a login")
	assert.NotContains(t, byLogin, "mysql.sys", "a reserved account is not a login")
	require.Contains(t, byLogin, "pu_analyst")
	assert.Equal(t, []string{"pu_reporter"}, byLogin["pu_analyst"].Roles)
	assert.Equal(t, "pu_reporter", byLogin["pu_analyst"].DefaultRole)
	assert.Equal(t, boolPtr(false), byLogin["pu_analyst"].Disabled)
	if comment != "" {
		assert.Equal(t, "runs the nightly load", byLogin["pu_analyst"].Comment)
	}
	require.Contains(t, byLogin, "pu_locked")
	assert.Equal(t, boolPtr(true), byLogin["pu_locked"].Disabled)

	noPriv, err := NewMySQLScrapper(ctx, &MySQLScrapperConf{MySQLConf: dwhexecmysql.MySQLConf{
		User:          "pu_nopriv",
		Password:      password,
		Host:          testenv.EnvOrDefault(envPrefix+"_HOST", ""),
		Port:          testenv.EnvOrDefaultInt(envPrefix+"_PORT", 3306),
		AllowInsecure: true,
	}})
	require.NoError(t, err)
	defer noPriv.Close()

	limited, err := noPriv.QueryPlatformUsers(ctx)
	require.NoError(t, err, "a login that may not read the mysql schema still lists itself")
	assertRefusedThenLimited(t, limited, mainSource, fallbackSource, "pu_nopriv")
}
