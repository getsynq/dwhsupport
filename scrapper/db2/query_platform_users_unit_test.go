package db2

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
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
	errRefused     = errors.New("db2: SQLCODE=-551 SQLSTATE=42501 SYNQ_READER, SELECT, SYSIBMADM.AUTHORIZATIONIDS")
	errNoView      = errors.New("db2: SQLCODE=-204 SQLSTATE=42704 SYSIBMADM.AUTHORIZATIONIDS")
	errNoColumn    = errors.New("db2: SQLCODE=-206 SQLSTATE=42703 SECADMAUTH")
	errNoRoutine   = errors.New("db2: SQLCODE=-440 SQLSTATE=42884 MON_GET_CONNECTION")
	errDisconnect  = errors.New("EOF")
	authIdColumns  = []string{"AUTHID"}
	roleAuthColumn = []string{"GRANTEE", "ROLENAME"}
)

func adminView(ids ...string) cannedAnswer {
	return authIdAnswer("SYSIBMADM.AUTHORIZATIONIDS", ids...)
}

func syscatUnion(ids ...string) cannedAnswer {
	return authIdAnswer("FROM SYSCAT.DBAUTH", ids...)
}

func sessionUser(id string) cannedAnswer {
	return authIdAnswer("SESSION_USER", id)
}

func authIdAnswer(match string, ids ...string) cannedAnswer {
	a := cannedAnswer{match: match, columns: authIdColumns}
	for _, id := range ids {
		a.rows = append(a.rows, []driver.Value{id})
	}
	return a
}

func roleAuthWith(rows ...[]driver.Value) cannedAnswer {
	return cannedAnswer{match: "AS rolename", columns: roleAuthColumn, rows: rows}
}

func failing(match string, err error) cannedAnswer { return cannedAnswer{match: match, err: err} }

func listWith(t *testing.T, answers ...cannedAnswer) (*scrapper.PlatformUsers, error) {
	return listDb2PlatformUsers(t.Context(), &cannedQuerier{t: t, answers: answers}, authIdSources)
}

func TestDb2ErrorClassification(t *testing.T) {
	assert.True(t, dwhexecdb2.IsPermissionError(errRefused))
	assert.False(t, isUnavailable(errRefused))
	for _, err := range []error{errNoView, errNoColumn, errNoRoutine, errors.New("SQLCODE:-204 diagnostic")} {
		assert.Truef(t, isUnavailable(err), "%v", err)
		assert.Falsef(t, dwhexecdb2.IsPermissionError(err), "%v", err)
	}
	assert.False(t, isUnavailable(errors.New("SQLCODE=-2040")), "the code is matched whole")
	assert.False(t, isUnavailable(errDisconnect))
	assert.False(t, isUnavailable(nil))
}

func TestDb2PlatformUsersFromTheAdminView(t *testing.T) {
	result, err := listWith(t,
		adminView("DB2INST1", "SVC"),
		sessionUser("SYNQ_READER"),
		roleAuthWith([]driver.Value{"SVC", "LOADER"}),
	)
	require.NoError(t, err)
	require.Len(t, result.Sources, 1, "the SYSCAT union is not read when the admin view answered")
	users := result.Source("db2.sysibmadm.authorizationids")
	require.True(t, users.Answered())
	require.Len(t, users.Users, 3, "the session user is added")
	assert.Equal(t, "SVC", users.Users[1].Login)
	assert.Equal(t, []string{"LOADER"}, users.Users[1].Roles)
	assert.Equal(t, "SYNQ_READER", users.Users[2].Login)
}

func TestDb2PlatformUsersFallBackToTheCatalog(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		state func(*scrapper.PlatformUserListing) string
	}{
		{"refused", errRefused, func(l *scrapper.PlatformUserListing) string { return l.Refused }},
		{"unavailable", errNoView, func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"failed", errDisconnect, func(l *scrapper.PlatformUserListing) string { return l.Failed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := listWith(t,
				failing("SYSIBMADM.AUTHORIZATIONIDS", tc.err),
				syscatUnion("DB2INST1"),
				sessionUser("DB2INST1"),
				roleAuthWith(),
			)
			require.NoError(t, err)
			require.Len(t, result.Sources, 2)
			assert.Equal(t, "db2.sysibmadm.authorizationids", result.Sources[0].Source)
			assert.NotEmpty(t, tc.state(result.Sources[0]))
			fallback := result.Sources[1]
			assert.Equal(t, "db2.syscat_auth", fallback.Source)
			require.True(t, fallback.Answered())
			assert.Len(t, fallback.Users, 1)
		})
	}
}

func TestDb2PlatformUsersNothingReadableIsAResult(t *testing.T) {
	result, err := listWith(t,
		failing("SYSIBMADM.AUTHORIZATIONIDS", errNoView),
		failing("FROM SYSCAT.DBAUTH", errRefused),
	)
	require.NoError(t, err, "a missing view or grant never fails the fetch")
	assert.False(t, result.Answered())
	reconciled := result.Reconcile()
	assert.NotEmpty(t, reconciled.Refused+reconciled.Unavailable)
}

func TestDb2PlatformUsersConnectionDownFails(t *testing.T) {
	_, err := listWith(t,
		failing("SYSIBMADM.AUTHORIZATIONIDS", errDisconnect),
		failing("FROM SYSCAT.DBAUTH", errDisconnect),
	)
	require.Error(t, err, "nothing answered and something failed outright")
}

func TestDb2PlatformUsersFactQueriesFailingNeverFailTheListing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason string
	}{
		{"refused", errRefused, "refused"},
		{"unavailable", errNoColumn, "this Db2"},
		{"failed", errDisconnect, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := listWith(t,
				adminView("DB2INST1"),
				failing("SESSION_USER", tc.err),
				failing("AS rolename", tc.err),
			)
			require.NoError(t, err)
			users := result.Source("db2.sysibmadm.authorizationids")
			require.True(t, users.Answered())
			require.Len(t, users.Users, 1, "a failed SESSION_USER only leaves it out")
			require.True(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
			for _, s := range users.SkippedFacts {
				if s.Fact == scrapper.PlatformUserFactRoles {
					assert.Contains(t, s.Reason, tc.reason)
				}
			}
		})
	}
}

func TestDb2PlatformUsersCancelledContextFails(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := listDb2PlatformUsers(ctx, &cannedQuerier{t: t, answers: []cannedAnswer{
		failing("SYSIBMADM.AUTHORIZATIONIDS", context.Canceled),
	}}, authIdSources)
	require.ErrorIs(t, err, context.Canceled)
}
