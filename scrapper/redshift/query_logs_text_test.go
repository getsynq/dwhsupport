package redshift

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/suite"
)

type QueryLogTextSuite struct {
	suite.Suite
}

func TestQueryLogTextSuite(t *testing.T) {
	suite.Run(t, new(QueryLogTextSuite))
}

func (s *QueryLogTextSuite) TestUnescapeQueryText() {
	cases := []struct {
		name, stored, want string
	}{
		{"empty", ``, ``},
		{"no backslash", "select\t1", "select\t1"},
		{"line feed", `select 1 -- c\nfrom t`, "select 1 -- c\nfrom t"},
		{"carriage return", `a\r\nb`, "a\r\nb"},
		{"doubled backslash", `'back\\slash'`, `'back\slash'`},
		{"doubled backslash before n is a backslash and an n", `'lit\\nchars'`, `'lit\nchars'`},
		{"three backslashes and n", `\\\n`, "\\\n"},
		{"unknown escape stays", `\t\x`, `\t\x`},
		{"trailing lone backslash stays", `abc\`, `abc\`},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			s.Equal(c.want, unescapeQueryText(c.stored))
		})
	}
}

func longText(prefix string) string {
	return prefix + strings.Repeat("x", queryTextChunkMinBytes)
}

func (s *QueryLogTextSuite) TestStatementText() {
	first := longText("select '")
	cases := []struct {
		name          string
		queryText     *string
		chunks        []queryTextChunk
		want          string
		wantTruncated bool
	}{
		{name: "no text", queryText: nil, want: ""},
		{name: "short text is complete without chunks", queryText: strPtr(`select 1\n`), want: "select 1\n"},
		{
			name:      "long text in one chunk is complete",
			queryText: &first,
			chunks:    []queryTextChunk{{0, first}},
			want:      first,
		},
		{
			name:      "chunks are joined in sequence order",
			queryText: &first,
			chunks:    []queryTextChunk{{1, "' as b"}, {0, first}},
			want:      first + "' as b",
		},
		{
			name:      "an escape split across chunks is joined before unescaping",
			queryText: strPtr(first + `\`),
			chunks:    []queryTextChunk{{0, first + `\`}, {1, `n'\`}, {2, `\x'`}},
			want:      first + "\n'\\x'",
		},
		{
			name:      "a character split across chunks is joined before decoding",
			queryText: strPtr(first + "\xc5"),
			chunks:    []queryTextChunk{{0, first + "\xc5"}, {1, "\xbc'"}},
			want:      first + "ż'",
		},
		{
			name:      "a duplicated chunk is read once",
			queryText: &first,
			chunks:    []queryTextChunk{{0, first}, {0, first}, {1, "'"}},
			want:      first + "'",
		},
		{
			name:          "long text without chunks is truncated",
			queryText:     &first,
			want:          first,
			wantTruncated: true,
		},
		{
			name:          "a missing chunk truncates at the gap",
			queryText:     &first,
			chunks:        []queryTextChunk{{0, first}, {1, "a"}, {3, "c"}},
			want:          first + "a",
			wantTruncated: true,
		},
		{
			name:          "without the first chunk only query_text is left",
			queryText:     &first,
			chunks:        []queryTextChunk{{1, "a"}},
			want:          first,
			wantTruncated: true,
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			text, truncated := statementText(&RedshiftQueryLogSchema{QueryText: c.queryText}, c.chunks)
			s.Equal(c.want, text)
			s.Equal(c.wantTruncated, truncated)
		})
	}
}

var queryLogColumns = []string{"query_id", "end_time", "query_text", "user_id", "user_name", "text_sequence", "text_chunk"}

func mockQuerier(s *QueryLogTextSuite, setup func(sqlmock.Sqlmock)) rowsQuerier {
	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		s.NoError(mock.ExpectationsWereMet())
		_ = db.Close()
	})
	setup(mock)
	return sqlx.NewDb(db, "sqlmock").QueryxContext
}

func (s *QueryLogTextSuite) collect(it querylogs.QueryLogIterator) []*querylogs.QueryLog {
	var logs []*querylogs.QueryLog
	for {
		log, err := it.Next(context.Background())
		if err == io.EOF {
			return logs
		}
		s.Require().NoError(err)
		logs = append(logs, log)
	}
}

func (s *QueryLogTextSuite) fetch(query rowsQuerier) []*querylogs.QueryLog {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)
	it, err := fetchQueryLogs(context.Background(), query, time.Time{}, time.Now(), obfuscator, "redshift", "host", "db")
	s.Require().NoError(err)
	return s.collect(it)
}

func (s *QueryLogTextSuite) TestIteratorAssemblesStatementsFromChunks() {
	end := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	first := longText("select '")
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), queryTextChunkMinBytes).
			WillReturnRows(sqlmock.NewRows(queryLogColumns).
				AddRow(1, end, `select 1\n`, 100, "etl_user  ", nil, nil).
				AddRow(2, end, first, 101, "analyst", 0, first).
				AddRow(2, end, first, 101, "analyst", 1, `' as a\n`).
				AddRow(3, end, first, nil, nil, nil, nil).
				AddRow(4, end, first, 100, "etl_user", 0, first).
				AddRow(5, end, `select 5`, 100, "etl_user", nil, nil))
	})

	logs := s.fetch(query)
	s.Require().Len(logs, 5)

	s.Equal([]string{"1", "2", "3", "4", "5"}, []string{logs[0].QueryID, logs[1].QueryID, logs[2].QueryID, logs[3].QueryID, logs[4].QueryID})

	s.Equal("select 1", logs[0].SQL)
	s.Equal("etl_user", logs[0].DwhContext.User)
	s.False(logs[0].IsTruncated)

	s.Equal(first+"' as a", logs[1].SQL)
	s.Equal("analyst", logs[1].DwhContext.User)
	s.False(logs[1].IsTruncated)

	// no chunks found for a long statement, and a user dropped since
	s.Equal(first, logs[2].SQL)
	s.True(logs[2].IsTruncated)
	s.Empty(logs[2].DwhContext.User)

	s.Equal(first, logs[3].SQL)
	s.False(logs[3].IsTruncated)

	s.Equal("select 5", logs[4].SQL)
}

// end_time is zone-less UTC, so bounds in another zone are sent as UTC.
func (s *QueryLogTextSuite) TestBoundsAreSentInUTC() {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	s.Require().NoError(err)
	from := time.Date(2025, 11, 1, 10, 0, 0, 0, warsaw)
	to := from.Add(time.Hour)
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").
			WithArgs(from.UTC(), to.UTC(), queryTextChunkMinBytes).
			WillReturnRows(sqlmock.NewRows(queryLogColumns))
	})
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)
	it, err := fetchQueryLogs(context.Background(), query, from, to, obfuscator, "redshift", "host", "db")
	s.Require().NoError(err)
	s.Empty(s.collect(it))
}

func (s *QueryLogTextSuite) TestIteratorOnEmptyResult() {
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").WillReturnRows(sqlmock.NewRows(queryLogColumns))
	})
	s.Empty(s.fetch(query))
}

func (s *QueryLogTextSuite) TestIteratorStopsOnCancelledContext() {
	end := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").
			WillReturnRows(sqlmock.NewRows(queryLogColumns).AddRow(1, end, "select 1", nil, nil, nil, nil))
	})
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)
	it, err := fetchQueryLogs(context.Background(), query, time.Time{}, time.Now(), obfuscator, "redshift", "host", "db")
	s.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = it.Next(ctx)
	s.ErrorIs(err, context.Canceled)
	_, err = it.Next(context.Background())
	s.ErrorIs(err, io.EOF)
}

// A cluster that cannot read SYS_QUERY_TEXT or pg_user still reports its query history: without user
// names, and with a statement long enough to have been cut flagged as truncated.
func (s *QueryLogTextSuite) TestFallsBackToHistoryAlone() {
	end := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	first := longText("select '")
	for _, code := range []pq.ErrorCode{"42P01", "42703", "42883", "42501", "0A000"} {
		s.Run(string(code), func() {
			query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SYS_QUERY_TEXT").WillReturnError(&pq.Error{Code: code})
				mock.ExpectQuery("FROM SYS_QUERY_HISTORY").
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnRows(sqlmock.NewRows([]string{"query_id", "end_time", "query_text", "user_id"}).
						AddRow(1, end, `select 1\nfrom t`, 100).
						AddRow(2, end, first, 100))
			})

			logs := s.fetch(query)
			s.Require().Len(logs, 2)
			s.Equal("select 1\nfrom t", logs[0].SQL)
			s.False(logs[0].IsTruncated)
			s.Empty(logs[0].DwhContext.User)
			s.True(logs[1].IsTruncated)
		})
	}
}

func (s *QueryLogTextSuite) TestOtherErrorsAreNotRetried() {
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").WillReturnError(driver.ErrBadConn)
	})
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)
	_, err = fetchQueryLogs(context.Background(), query, time.Time{}, time.Now(), obfuscator, "redshift", "host", "db")
	s.Error(err)
}

func (s *QueryLogTextSuite) TestQueryLogsSqlEmbedsTheHistoryQuery() {
	s.Contains(queryLogsSql, "FROM SYS_QUERY_HISTORY")
	s.NotContains(queryLogsSql, "/* SYS_QUERY_HISTORY */")
}
