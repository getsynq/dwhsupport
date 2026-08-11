package snowflake

import (
	"context"
	"os"
	"strings"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/stretchr/testify/require"
)

// Regression: an unreachable configured database must come back as a named warning,
// not as a connect failure.
func TestValidateConfigurationReportsUnreachableDatabase(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake integration test in CI")
	}
	// Skip only when the connection is not configured at all. Once it is, a failure to
	// connect is a real result rather than a reason to opt out — that failure is exactly
	// the regression this test exists to catch.
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" || os.Getenv("SNOWFLAKE_USER") == "" ||
		os.Getenv("SNOWFLAKE_DATABASE") == "" || os.Getenv("SNOWFLAKE_WAREHOUSE") == "" {
		t.Skip("Snowflake connection not configured")
	}
	if os.Getenv("SNOWFLAKE_PASSWORD") == "" &&
		os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE") == "" &&
		os.Getenv("SNOWFLAKE_PRIVATE_KEY") == "" {
		t.Skip("no Snowflake credential configured")
	}

	const unreachable = "SYNQ_NO_SUCH_DATABASE"
	// Snowflake canonicalises unquoted identifiers to upper case, and the validation
	// compares against the names SHOW DATABASES returns. A lower-cased env value would
	// otherwise put the good database into the missing set too, and the test would pass
	// while exercising the wrong scenario.
	database := strings.ToUpper(os.Getenv("SNOWFLAKE_DATABASE"))

	sfConf := dwhexecsnowflake.SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Role:           os.Getenv("SNOWFLAKE_ROLE"),
		PrivateKeyFile: os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
		// Unreachable entry first: that is the one promoted to session default.
		Databases: []string{unreachable, database},
	}
	if pk := os.Getenv("SNOWFLAKE_PRIVATE_KEY"); pk != "" {
		sfConf.PrivateKey = []byte(pk)
	}

	ctx := context.Background()
	sc, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: sfConf})
	require.NoError(t, err, "connection must survive an unreachable session default database")
	defer func() { _ = sc.Close() }()

	warnings, err := sc.ValidateConfiguration(ctx)
	require.NoError(t, err)

	// Exact match: a warning that also named the reachable database would not equal this.
	require.Contains(t, warnings, "Database not found or no permissions to access: "+unreachable,
		"expected a warning naming only %q, got %v", unreachable, warnings)

	// Prove the degraded session can still do real work. QueryTables reads each
	// database's information_schema, so unlike an account-level SHOW it would fail if
	// dropping the session default had left the connection unusable.
	tables, err := sc.QueryTables(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, tables, "the reachable database must still be queryable")
	for _, tbl := range tables {
		require.Equal(t, database, tbl.Database, "only the reachable database should be scraped")
	}
}
