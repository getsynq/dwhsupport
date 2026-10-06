package snowflake

import (
	"context"
	"os"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func snowflakeTestConf() dwhexecsnowflake.SnowflakeConf {
	conf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{os.Getenv("SNOWFLAKE_DATABASE")},
		Role:           os.Getenv("SNOWFLAKE_ROLE"),
		PrivateKeyFile: os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
	}
	if pk := os.Getenv("SNOWFLAKE_PRIVATE_KEY"); pk != "" {
		conf.PrivateKey = []byte(pk)
	}
	return conf
}

func skipWithoutSnowflake(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake integration tests in CI")
	}
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" || os.Getenv("SNOWFLAKE_DATABASE") == "" {
		t.Skip("SNOWFLAKE_ACCOUNT / SNOWFLAKE_DATABASE not set")
	}
}

type SnowflakePlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

// The test account's role reads ACCOUNT_USAGE, so the listing is the
// complete one with every fact the test user has.
func TestSnowflakePlatformUsersSuite(t *testing.T) {
	skipWithoutSnowflake(t)
	sc, err := NewSnowflakeScrapper(context.Background(), &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer sc.Close()

	s := new(SnowflakePlatformUsersSuite)
	s.Scrapper = sc
	s.ConnectedLogin = os.Getenv("SNOWFLAKE_USER")
	s.MatchLoginFold = true
	s.ExpectFacts = []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactPlatformId,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactCreatedAt,
		scrapper.PlatformUserFactDefaultRole,
		scrapper.PlatformUserFactRoles,
	}
	suite.Run(t, s)
}

// On an account where ACCOUNT_USAGE is granted both sources answer: the
// complete one with every fact, and SHOW USERS naming the same users.
func TestSnowflakePlatformUsers_ReadsBothSources(t *testing.T) {
	skipWithoutSnowflake(t)
	ctx := context.Background()
	sc, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer sc.Close()

	result, err := sc.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	require.Len(t, result.Sources, 2)
	accountUsage, show := result.Sources[0], result.Sources[1]
	require.Equal(t, accountUsageUsersSource, accountUsage.Source)
	require.Equal(t, showUsersSource, show.Source)
	require.Empty(t, accountUsage.Refused)
	require.Empty(t, show.Refused)
	require.Equal(t, scrapper.PlatformUsersComplete, accountUsage.Completeness)
	require.Equal(t, scrapper.PlatformUsersUnknown, show.Completeness)

	// SHOW USERS names every user ACCOUNT_USAGE has, give or take the users
	// created or dropped within ACCOUNT_USAGE's latency.
	showLogins := map[string]bool{}
	for _, u := range show.Users {
		showLogins[u.Login] = true
		require.NotNil(t, u.CreatedAt, "SHOW USERS gives created_on even for users the role does not own: %s", u.Login)
	}
	missingFromShow := 0
	for _, u := range accountUsage.Users {
		if !showLogins[u.Login] {
			missingFromShow++
		}
	}
	require.LessOrEqual(t, missingFromShow, 1, "SHOW USERS lists %d users, ACCOUNT_USAGE %d", len(show.Users), len(accountUsage.Users))

	reconciled := result.Reconcile()
	require.Equal(t, scrapper.PlatformUsersComplete, reconciled.Completeness)
	require.GreaterOrEqual(t, len(reconciled.Users), len(accountUsage.Users))
	require.False(t, reconciled.IsSkipped(scrapper.PlatformUserFactRoles), "ACCOUNT_USAGE gives the roles: %v", reconciled.SkippedFacts)
}

// An ACCOUNT_USAGE database that does not exist is refused exactly like one the
// role may not read ("does not exist or not authorized"), so this drives the
// refused source end to end on an account where ACCOUNT_USAGE is granted.
func TestSnowflakePlatformUsers_RefusedAccountUsageLeavesShowUsers(t *testing.T) {
	skipWithoutSnowflake(t)
	ctx := context.Background()
	missing := "NO_SUCH_ACCOUNT_USAGE_DB"
	sc, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf(), AccountUsageDb: &missing})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer sc.Close()

	result, err := sc.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	require.Len(t, result.Sources, 2)
	require.NotEmpty(t, result.Source(accountUsageUsersSource).Refused)
	require.Empty(t, result.Source(accountUsageUsersSource).Users)
	require.NotEmpty(t, result.Source(showUsersSource).Users)

	reconciled := result.Reconcile()
	require.Equal(t, scrapper.PlatformUsersUnknown, reconciled.Completeness)
	require.Contains(t, reconciled.CompletenessReason, accountUsageUsersSource+" refused")
	require.True(t, reconciled.IsSkipped(scrapper.PlatformUserFactRoles))
	require.True(t, reconciled.IsSkipped(scrapper.PlatformUserFactPlatformId))
}

// A refused source is told apart from a failure by IsPermissionError, so a refused ACCOUNT_USAGE.USERS must
// be one IsPermissionError recognises.
func TestSnowflakePlatformUsers_RefusedAccountUsageIsPermissionError(t *testing.T) {
	skipWithoutSnowflake(t)
	ctx := context.Background()
	missing := "NO_SUCH_ACCOUNT_USAGE_DB"
	refused, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf(), AccountUsageDb: &missing})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer refused.Close()
	_, err = refused.queryAccountUsageUsers(ctx)
	require.Error(t, err)
	require.True(t, refused.IsPermissionError(err), "a refused ACCOUNT_USAGE.USERS is a permission error: %v", err)
}
