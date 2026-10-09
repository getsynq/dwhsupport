package mssql

import (
	"context"
	"os"
	"testing"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MSSQLPlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestMSSQLPlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL platform users tests in CI")
	}
	suite.Run(t, new(MSSQLPlatformUsersSuite))
}

func (s *MSSQLPlatformUsersSuite) SetupSuite() {
	sc, err := newMSSQLScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to MSSQL: %v", err)
	}
	s.Scrapper = sc
	s.ConnectedLogin = testenv.EnvOrDefault("MSSQL_USER", "synq")
	s.ExpectFacts = []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactPlatformId,
		scrapper.PlatformUserFactType,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactCreatedAt,
	}
}

func (s *MSSQLPlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func newMSSQLScrapperAs(ctx context.Context, user, password string) (*MSSQLScrapper, error) {
	return NewMSSQLScrapper(ctx, &MSSQLScrapperConf{MSSQLConf: dwhexecmssql.MSSQLConf{
		User:      user,
		Password:  password,
		Host:      testenv.EnvOrDefault("MSSQL_HOST", "127.0.0.1"),
		Port:      testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
		Database:  testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test"),
		TrustCert: true,
		Encrypt:   testenv.EnvOrDefault("MSSQL_ENCRYPT", "disable"),
	}})
}

// TestMSSQLPlatformUsers_LoginsRolesAndVisibility creates, as sa, a disabled
// login, a login holding a server role and a database role, and a login with
// no permissions, then checks what sa sees and what the unprivileged login
// sees: a limited listing of itself and sa, never an error.
func TestMSSQLPlatformUsers_LoginsRolesAndVisibility(t *testing.T) {
	saPassword := os.Getenv("MSSQL_SA_PASSWORD")
	if os.Getenv("CI") != "" || saPassword == "" {
		t.Skip("MSSQL_SA_PASSWORD not set")
	}
	ctx := context.Background()
	sa, err := newMSSQLScrapperAs(ctx, "sa", saPassword)
	if err != nil {
		t.Skipf("Could not connect to MSSQL as sa: %v", err)
	}
	defer sa.Close()
	db := sa.executor.GetDb()

	const password = "PlatformUsers1!"
	drop := []string{
		"IF USER_ID('pu_reader') IS NOT NULL DROP USER pu_reader",
		"IF SUSER_ID('pu_reader') IS NOT NULL DROP LOGIN pu_reader",
		"IF SUSER_ID('pu_disabled') IS NOT NULL DROP LOGIN pu_disabled",
		"IF SUSER_ID('pu_nopriv') IS NOT NULL DROP LOGIN pu_nopriv",
	}
	setup := append(append([]string{}, drop...),
		"CREATE LOGIN pu_reader WITH PASSWORD = '"+password+"', CHECK_POLICY = OFF",
		"ALTER SERVER ROLE dbcreator ADD MEMBER pu_reader",
		"CREATE USER pu_reader FOR LOGIN pu_reader",
		"ALTER ROLE db_datareader ADD MEMBER pu_reader",
		"CREATE LOGIN pu_disabled WITH PASSWORD = '"+password+"', CHECK_POLICY = OFF",
		"ALTER LOGIN pu_disabled DISABLE",
		"CREATE LOGIN pu_nopriv WITH PASSWORD = '"+password+"', CHECK_POLICY = OFF",
	)
	for _, stmt := range setup {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, stmt := range drop {
			_, _ = db.ExecContext(context.Background(), stmt)
		}
	})

	users, err := scrappertest.OnlyPlatformUserSource(sa.QueryPlatformUsers(ctx))
	require.NoError(t, err)
	assert.Equal(t, scrapper.PlatformUsersComplete, users.Completeness)
	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}
	assert.NotContains(t, byLogin, "sysadmin", "a server role is not a login")
	assert.NotContains(t, byLogin, "##MS_PolicyEventProcessingLogin##", "the server's own logins are left out")
	require.Contains(t, byLogin, "sa")
	assert.Contains(t, byLogin["sa"].Roles, "sysadmin")
	require.Contains(t, byLogin, "pu_reader")
	dbName := testenv.EnvOrDefault("MSSQL_DATABASE", "synq_test")
	assert.Equal(t, []string{"dbcreator", dbName + ".db_datareader"}, byLogin["pu_reader"].Roles)
	assert.Equal(t, "SQL_LOGIN", byLogin["pu_reader"].Type)
	require.NotNil(t, byLogin["pu_reader"].Disabled)
	assert.False(t, *byLogin["pu_reader"].Disabled)
	require.Contains(t, byLogin, "pu_disabled")
	require.NotNil(t, byLogin["pu_disabled"].Disabled)
	assert.True(t, *byLogin["pu_disabled"].Disabled)
	scrappertest.AssertSkipped(t, users, scrapper.PlatformUserFactEmail, scrapper.PlatformUserSkipUnavailable)
	scrappertest.AssertSkipped(t, users, scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserSkipUnavailable)

	noPriv, err := NewMSSQLScrapper(ctx, &MSSQLScrapperConf{MSSQLConf: dwhexecmssql.MSSQLConf{
		User:      "pu_nopriv",
		Password:  password,
		Host:      testenv.EnvOrDefault("MSSQL_HOST", "127.0.0.1"),
		Port:      testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
		Database:  "master",
		TrustCert: true,
		Encrypt:   testenv.EnvOrDefault("MSSQL_ENCRYPT", "disable"),
	}})
	require.NoError(t, err)
	defer noPriv.Close()

	limited, err := scrappertest.OnlyPlatformUserSource(noPriv.QueryPlatformUsers(ctx))
	require.NoError(t, err, "the catalog views are readable by every login")
	assert.Equal(t, scrapper.PlatformUsersLimited, limited.Completeness)
	assert.Contains(t, limited.CompletenessReason, "VIEW ANY DEFINITION")
	logins := []string{}
	for _, u := range limited.Users {
		logins = append(logins, u.Login)
	}
	assert.Contains(t, logins, "pu_nopriv")
	assert.NotContains(t, logins, "pu_reader", "another login is hidden without VIEW ANY DEFINITION")
}

// TestMSSQLPlatformUsers_DatabaseCollationDiffersFromServer connects to a
// database whose collation is not the server's. Login names in
// sys.server_principals carry the server's collation and user names in
// sys.database_principals the database's, so a statement that puts the two in
// one column has to say which collation the column takes.
func TestMSSQLPlatformUsers_DatabaseCollationDiffersFromServer(t *testing.T) {
	saPassword := os.Getenv("MSSQL_SA_PASSWORD")
	if os.Getenv("CI") != "" || saPassword == "" {
		t.Skip("MSSQL_SA_PASSWORD not set")
	}
	ctx := context.Background()
	sa, err := newMSSQLScrapperAs(ctx, "sa", saPassword)
	if err != nil {
		t.Skipf("Could not connect to MSSQL as sa: %v", err)
	}
	defer sa.Close()
	db := sa.executor.GetDb()

	var serverCollation string
	require.NoError(t, db.GetContext(ctx, &serverCollation, "SELECT CAST(SERVERPROPERTY('Collation') AS nvarchar(128))"))
	dbCollation := "Latin1_General_CS_AS"
	if serverCollation == dbCollation {
		dbCollation = "SQL_Latin1_General_CP1_CI_AS"
	}

	const dbName = "pu_collation"
	drop := "IF DB_ID('" + dbName + "') IS NOT NULL BEGIN " +
		"ALTER DATABASE " + dbName + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE " + dbName + " END"
	_, err = db.ExecContext(ctx, drop)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE DATABASE "+dbName+" COLLATE "+dbCollation)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), drop) })

	inDb, err := NewMSSQLScrapper(ctx, &MSSQLScrapperConf{MSSQLConf: dwhexecmssql.MSSQLConf{
		User:      "sa",
		Password:  saPassword,
		Host:      testenv.EnvOrDefault("MSSQL_HOST", "127.0.0.1"),
		Port:      testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
		Database:  dbName,
		TrustCert: true,
		Encrypt:   testenv.EnvOrDefault("MSSQL_ENCRYPT", "disable"),
	}})
	require.NoError(t, err)
	defer inDb.Close()

	users, err := scrappertest.OnlyPlatformUserSource(inDb.QueryPlatformUsers(ctx))
	require.NoError(t, err)
	require.True(t, users.Answered())
	assert.Equal(t, scrapper.PlatformUsersComplete, users.Completeness)
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactRoles), "role memberships are read too: %v", users.SkippedFacts)
	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}
	require.Contains(t, byLogin, "sa")
	assert.Contains(t, byLogin["sa"].Roles, "sysadmin")
	assert.Contains(t, byLogin["sa"].Roles, dbName+".db_owner", "sa is the owner of the database it created")
}
