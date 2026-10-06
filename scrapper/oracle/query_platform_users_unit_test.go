package oracle

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cannedAnswer answers every statement containing match, with rows or an error.
type cannedAnswer struct {
	match   string
	err     error
	columns []string
	rows    [][]driver.Value
}

// cannedQuerier answers each statement with the first cannedAnswer that
// matches it, and fails a statement nothing matches.
type cannedQuerier struct {
	t       *testing.T
	answers []cannedAnswer
}

func (c *cannedQuerier) QueryRows(ctx context.Context, query string, _ ...interface{}) (*sqlx.Rows, error) {
	for _, a := range c.answers {
		if !strings.Contains(query, a.match) {
			continue
		}
		if a.err != nil {
			return nil, a.err
		}
		db, mock, err := sqlmock.New()
		require.NoError(c.t, err)
		c.t.Cleanup(func() { _ = db.Close() })
		canned := sqlmock.NewRows(a.columns)
		for _, r := range a.rows {
			canned.AddRow(r...)
		}
		mock.ExpectQuery(".").WillReturnRows(canned)
		return sqlx.NewDb(db, "sqlmock").QueryxContext(ctx, "SELECT 1")
	}
	c.t.Fatalf("unexpected statement: %s", query)
	return nil, nil
}

var (
	errRefused     = errors.New(`ORA-00942: table or view "SYNQ"."DBA_USERS" does not exist`)
	errNoColumn    = errors.New(`ORA-00904: "LAST_LOGIN": invalid identifier`)
	errNoObject    = errors.New(`ORA-04043: object DBA_ROLE_PRIVS does not exist`)
	errDisconnect  = errors.New(`ORA-03113: end-of-file on communication channel`)
	created        = time.Date(2024, 3, 1, 10, 0, 0, 0, time.UTC)
	fullDbaColumns = []string{"USERNAME", "USER_ID", "ACCOUNT_STATUS", "AUTHENTICATION_TYPE", "CREATED_UTC", "LAST_LOGIN_UTC"}
)

func dbaUsersFull() cannedAnswer {
	return cannedAnswer{match: "last_login", columns: fullDbaColumns, rows: [][]driver.Value{
		{"SYNQ", int64(1), "OPEN", "PASSWORD", created, created},
		{"SVC", int64(2), "LOCKED", "PASSWORD", created, nil},
	}}
}

func dbaUsersBase() cannedAnswer {
	return cannedAnswer{match: "FROM DBA_USERS", columns: []string{"USERNAME", "USER_ID", "ACCOUNT_STATUS", "CREATED_UTC"}, rows: [][]driver.Value{
		{"SYNQ", int64(1), "OPEN", created},
	}}
}

func allUsers() cannedAnswer {
	return cannedAnswer{match: "FROM ALL_USERS", columns: []string{"USERNAME", "USER_ID", "CREATED_UTC"}, rows: [][]driver.Value{
		{"SYNQ", int64(1), created},
		{"SVC", int64(2), created},
	}}
}

func roleGrants() cannedAnswer {
	return cannedAnswer{match: "DBA_ROLE_PRIVS", columns: []string{"GRANTEE", "GRANTED_ROLE"}, rows: [][]driver.Value{
		{"SVC", "LOADER"}, {"NOT_A_USER_ROLE", "CONNECT"},
	}}
}

func failing(match string, err error) cannedAnswer { return cannedAnswer{match: match, err: err} }

func listWith(t *testing.T, answers ...cannedAnswer) (*scrapper.PlatformUsers, error) {
	return listOraclePlatformUsers(t.Context(), &cannedQuerier{t: t, answers: answers}, defaultPlatformUserViews)
}

func TestOracleErrorClassification(t *testing.T) {
	assert.True(t, dwhexecoracle.IsPermissionError(errRefused), "ORA-00942 is a refusal: a grant usually fixes it")
	assert.False(t, isUnavailable(errRefused))
	assert.True(t, isUnavailable(errNoColumn))
	assert.True(t, isUnavailable(errNoObject))
	assert.False(t, dwhexecoracle.IsPermissionError(errNoColumn))
	assert.False(t, isUnavailable(errDisconnect))
	assert.False(t, dwhexecoracle.IsPermissionError(errDisconnect))
	assert.False(t, isUnavailable(nil))
}

func TestOraclePlatformUsersFromDbaUsers(t *testing.T) {
	result, err := listWith(t, dbaUsersFull(), roleGrants())
	require.NoError(t, err)
	require.Len(t, result.Sources, 1, "ALL_USERS is not read when DBA_USERS answered")
	users := result.Source(oracleDbaUsersSource)
	require.True(t, users.Answered())
	require.Len(t, users.Users, 2)
	assert.Equal(t, []string{"LOADER"}, users.Users[0].Roles, "SVC sorts first")
	assert.True(t, *users.Users[0].Disabled)
	assert.Equal(t, created, *users.Users[1].LastLoginAt)
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactLastLoginAt))
}

func TestOraclePlatformUsersOlderVersionDropsTheMissingColumns(t *testing.T) {
	result, err := listWith(t, failing("last_login", errNoColumn), dbaUsersBase(), roleGrants())
	require.NoError(t, err)
	require.Len(t, result.Sources, 1, "an ORA-00904 is retried on DBA_USERS, not answered by ALL_USERS")
	users := result.Source(oracleDbaUsersSource)
	require.True(t, users.Answered())
	require.Len(t, users.Users, 1)
	assert.NotNil(t, users.Users[0].Disabled, "ACCOUNT_STATUS is still read")
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactLastLoginAt))
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactType))
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactDisabled))
}

func TestOraclePlatformUsersFallBackToAllUsers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		state func(*scrapper.PlatformUserListing) string
	}{
		{"refused", errRefused, func(l *scrapper.PlatformUserListing) string { return l.Refused }},
		{"unavailable", errNoObject, func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"failed", errDisconnect, func(l *scrapper.PlatformUserListing) string { return l.Failed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// errNoObject on the full query is retried on the base one, which fails the same way.
			result, err := listWith(t, failing("FROM DBA_USERS", tc.err), allUsers(), roleGrants())
			require.NoError(t, err)
			require.Len(t, result.Sources, 2)
			main := result.Sources[0]
			assert.Equal(t, oracleDbaUsersSource, main.Source)
			assert.NotEmpty(t, tc.state(main), "DBA_USERS is %s", tc.name)
			assert.False(t, main.Answered())

			fallback := result.Sources[1]
			assert.Equal(t, oracleAllUsersSource, fallback.Source)
			require.True(t, fallback.Answered())
			assert.Len(t, fallback.Users, 2)
			assert.Equal(t, scrapper.PlatformUsersComplete, fallback.Completeness)
			assert.True(t, fallback.IsSkipped(scrapper.PlatformUserFactDisabled))
		})
	}
}

func TestOraclePlatformUsersEveryViewRefusedIsAResult(t *testing.T) {
	result, err := listWith(t, failing("FROM DBA_USERS", errRefused), failing("FROM ALL_USERS", errRefused))
	require.NoError(t, err, "a missing grant never fails the fetch")
	assert.False(t, result.Answered())
	assert.NotEmpty(t, result.Reconcile().Refused)
}

func TestOraclePlatformUsersConnectionDownFails(t *testing.T) {
	_, err := listWith(t, failing("FROM DBA_USERS", errDisconnect), failing("FROM ALL_USERS", errDisconnect))
	require.Error(t, err, "nothing answered and something failed outright")
}

func TestOraclePlatformUsersRoleGrantsFailingNeverFailTheListing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason string
	}{
		{"refused", errRefused, "refused"},
		{"unavailable", errNoObject, "this Oracle version"},
		{"failed", errDisconnect, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := listWith(t, dbaUsersFull(), failing("DBA_ROLE_PRIVS", tc.err))
			require.NoError(t, err)
			users := result.Source(oracleDbaUsersSource)
			require.True(t, users.Answered())
			assert.Len(t, users.Users, 2)
			require.True(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
			for _, s := range users.SkippedFacts {
				if s.Fact == scrapper.PlatformUserFactRoles {
					assert.Contains(t, s.Reason, tc.reason)
				}
			}
		})
	}
}

func TestOraclePlatformUsersCancelledContextFails(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := listOraclePlatformUsers(ctx, &cannedQuerier{t: t, answers: []cannedAnswer{
		failing("FROM DBA_USERS", context.Canceled), failing("FROM ALL_USERS", context.Canceled),
	}}, defaultPlatformUserViews)
	require.ErrorIs(t, err, context.Canceled)
}
