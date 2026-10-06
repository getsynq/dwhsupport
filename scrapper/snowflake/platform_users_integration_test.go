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

// An ACCOUNT_USAGE database that does not exist is refused exactly like one the
// role may not read ("does not exist or not authorized"), so this drives the
// SHOW USERS fallback end to end on an account where ACCOUNT_USAGE is granted.
func TestSnowflakePlatformUsers_FallsBackToShowUsers(t *testing.T) {
	skipWithoutSnowflake(t)
	ctx := context.Background()
	missing := "NO_SUCH_ACCOUNT_USAGE_DB"

	complete, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer complete.Close()
	fallback, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf(), AccountUsageDb: &missing})
	require.NoError(t, err)
	defer fallback.Close()

	want, err := complete.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	require.Equal(t, scrapper.PlatformUsersComplete, want.Completeness)

	got, err := fallback.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	require.Equal(t, scrapper.PlatformUsersUnknown, got.Completeness)
	require.Contains(t, got.CompletenessReason, "SHOW USERS")
	require.True(t, got.IsSkipped(scrapper.PlatformUserFactRoles))
	require.True(t, got.IsSkipped(scrapper.PlatformUserFactPlatformId))

	// SHOW USERS names every user ACCOUNT_USAGE has, give or take the users
	// created or dropped within ACCOUNT_USAGE's latency.
	gotLogins := map[string]bool{}
	for _, u := range got.Users {
		gotLogins[u.Login] = true
	}
	missingFromShow := 0
	for _, u := range want.Users {
		if !gotLogins[u.Login] {
			missingFromShow++
		}
	}
	require.LessOrEqual(t, missingFromShow, 1, "SHOW USERS lists %d users, ACCOUNT_USAGE %d", len(got.Users), len(want.Users))
	for _, u := range got.Users {
		require.NotNil(t, u.CreatedAt, "SHOW USERS gives created_on even for users the role does not own: %s", u.Login)
	}
}

// The fallback is taken only on a refusal, so a refused ACCOUNT_USAGE.USERS must
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
