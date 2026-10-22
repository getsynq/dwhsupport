package redshift

import (
	"context"
	"database/sql/driver"
	"fmt"
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

// reconHashColumns is the text of a synq-recon row hash over columns from..to-1, the shape of statement
// Redshift cut into chunks shorter than queryTextChunkMinBytes: every column adds three pipes and
// four single quotes.
func reconHashColumns(from, to int) string {
	var b strings.Builder
	for i := from; i < to; i++ {
		fmt.Fprintf(&b, `COALESCE(CAST("c%d" AS VARCHAR), '<NULL>') || '|' || `, i)
	}
	return b.String()
}

// reconFirstChunk is the start of a long synq-recon hash query cut where Redshift cuts it: 4000 bytes
// with each pipe counted twice, which leaves about 3660 bytes and ends inside a string literal.
func reconFirstChunk() (first, rest string) {
	text := `select "id" as pk, "loaded_at_utc" as loaded_at_ts, (STRTOL(SUBSTRING(MD5(` + reconHashColumns(0, 200) +
		`''), 1, 8), 16)) as h from t`
	stored := 0
	for i := range len(text) {
		stored++
		if text[i] == '|' {
			stored++
		}
		if stored >= 4000 {
			return text[:i], text[i:]
		}
	}
	panic("recon text shorter than a chunk")
}

func (s *QueryLogTextSuite) TestReconFirstChunkIsShorterThanTheThreshold() {
	first, _ := reconFirstChunk()
	s.Less(len(first), queryTextChunkMinBytes)
	s.Greater(len(first), 3600)
	s.True(strings.HasSuffix(first, `'<NULL>') || '`), first[len(first)-20:])
}

func (s *QueryLogTextSuite) TestStatementText() {
	reconFirst, reconRest := reconFirstChunk()
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
			name:      "a first chunk shorter than the threshold is joined with the next one",
			queryText: &reconFirst,
			chunks:    []queryTextChunk{{0, reconFirst}, {1, reconRest}},
			want:      reconFirst + reconRest,
		},
		{
			// query_logs.sql reads the chunks of every statement SYS_QUERY_TEXT has a second chunk of.
			name:      "long text without chunks is complete",
			queryText: strPtr(longText("select ")),
			want:      longText("select "),
		},
		{
			name:          "text ending inside a quote without chunks is truncated",
			queryText:     &reconFirst,
			want:          reconFirst,
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
			text, truncated := statementText(&RedshiftQueryLogSchema{QueryText: c.queryText}, c.chunks, true)
			s.Equal(c.want, text)
			s.Equal(c.wantTruncated, truncated)
		})
	}
}

// Without SYS_QUERY_TEXT, query_text is all there is, and only a text that cannot have been cut is
// complete.
func (s *QueryLogTextSuite) TestStatementTextWithoutTheChunks() {
	reconFirst, _ := reconFirstChunk()
	pipes := strings.Repeat("'|' || ", 3800/8)
	cases := []struct {
		name          string
		queryText     *string
		want          string
		wantTruncated bool
	}{
		{name: "no text", queryText: nil, want: ""},
		{name: "short text", queryText: strPtr(`select 1\nfrom t`), want: "select 1\nfrom t"},
		{name: "long text", queryText: strPtr(longText("select ")), want: longText("select "), wantTruncated: true},
		{name: "a first chunk Redshift cut inside a literal", queryText: &reconFirst, want: reconFirst, wantTruncated: true},
		{name: "a literal left open", queryText: strPtr(`select 'abc`), want: `select 'abc`, wantTruncated: true},
		{
			name:          "a chunk of pipes as Redshift counts it",
			queryText:     strPtr(`select ` + pipes + `'x'`),
			want:          `select ` + pipes + `'x'`,
			wantTruncated: true,
		},
		{
			name:      "an escaped line feed ends a line comment",
			queryText: strPtr(`select 1 -- it's\nfrom t`),
			want:      "select 1 -- it's\nfrom t",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			text, truncated := statementText(&RedshiftQueryLogSchema{QueryText: c.queryText}, nil, false)
			s.Equal(c.want, text)
			s.Equal(c.wantTruncated, truncated)
		})
	}
}

func (s *QueryLogTextSuite) TestEndsInsideQuote() {
	cases := []struct {
		text string
		want bool
	}{
		{``, false},
		{`select 1`, false},
		{`select 'a'`, false},
		{`select ''`, false},
		{`select ''''`, false},
		{`select 'it''s'`, false},
		{`select 'it''s`, true},
		{`select 'a`, true},
		{`select '`, true},
		{`select 'a\'b'`, false},
		{`select 'a\'`, true},
		{`select 'a\`, true},
		{`select "c68" from t`, false},
		{`select "c68`, true},
		{`select "a""b"`, false},
		{`select "a""b`, true},
		{`select "it's" from t`, false},
		{`select 1 -- it's`, false},
		{"select 1 -- it's\nfrom t", false},
		{"select 1 -- c\n, 'a", true},
		{`select 1 /* it's */ from t`, false},
		{`select 1 /* it's`, true},
		{`select 1 /* "a */`, false},
		{`select $$a'b$$`, false},
		{`select $$a'b`, true},
		{`select $body$ $$ $body$`, false},
		{`select $body$ $$`, true},
		{`select $1, '$$'`, false},
		{`select $1 from t where a = $2`, false},
		{`select a$b from t`, false},
		{`select 1 /* {"app":"synq"} */`, false},
	}
	for _, c := range cases {
		s.Equal(c.want, endsInsideQuote(c.text), c.text)
	}
}

func (s *QueryLogTextSuite) TestStoredChunkBytesCountsEachPipeTwice() {
	s.Equal(0, storedChunkBytes(""))
	s.Equal(8, storedChunkBytes("select 1"))
	s.Equal(11, storedChunkBytes("'|' || x"))
	first, _ := reconFirstChunk()
	s.Equal(3999, storedChunkBytes(first))
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
	reconFirst, reconRest := reconFirstChunk()
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows(queryLogColumns).
				AddRow(1, end, `select 1\n`, 100, "etl_user  ", nil, nil).
				AddRow(2, end, first, 101, "analyst", 0, first).
				AddRow(2, end, first, 101, "analyst", 1, `' as a\n`).
				AddRow(3, end, longText("select "), nil, nil, nil, nil).
				AddRow(4, end, first, 100, "etl_user", 0, first).
				AddRow(5, end, `select 5`, 100, "etl_user", nil, nil).
				AddRow(6, end, reconFirst, 100, "etl_user", 0, reconFirst).
				AddRow(6, end, reconFirst, 100, "etl_user", 1, reconRest))
	})

	logs := s.fetch(query)
	s.Require().Len(logs, 6)

	s.Equal([]string{"1", "2", "3", "4", "5", "6"},
		[]string{logs[0].QueryID, logs[1].QueryID, logs[2].QueryID, logs[3].QueryID, logs[4].QueryID, logs[5].QueryID})

	s.Equal("select 1", logs[0].SQL)
	s.Equal("etl_user", logs[0].DwhContext.User)
	s.False(logs[0].IsTruncated)

	s.Equal(first+"' as a", logs[1].SQL)
	s.Equal("analyst", logs[1].DwhContext.User)
	s.False(logs[1].IsTruncated)

	// a long statement SYS_QUERY_TEXT holds no second chunk of, and a user dropped since
	s.Equal(longText("select "), logs[2].SQL)
	s.False(logs[2].IsTruncated)
	s.Empty(logs[2].DwhContext.User)

	s.Equal(first, logs[3].SQL)
	s.False(logs[3].IsTruncated)

	s.Equal("select 5", logs[4].SQL)

	// a first chunk Redshift cut short, as it does on a statement full of pipes
	s.Equal(strings.TrimSpace(reconFirst+reconRest), logs[5].SQL)
	s.False(logs[5].IsTruncated)
}

// end_time is zone-less UTC, so bounds in another zone are sent as UTC.
func (s *QueryLogTextSuite) TestBoundsAreSentInUTC() {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	s.Require().NoError(err)
	from := time.Date(2025, 11, 1, 10, 0, 0, 0, warsaw)
	to := from.Add(time.Hour)
	query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SYS_QUERY_TEXT").
			WithArgs(from.UTC(), to.UTC()).
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
	reconFirst, _ := reconFirstChunk()
	for _, code := range []pq.ErrorCode{"42P01", "42703", "42883", "42501", "0A000"} {
		s.Run(string(code), func() {
			query := mockQuerier(s, func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SYS_QUERY_TEXT").WillReturnError(&pq.Error{Code: code})
				mock.ExpectQuery("FROM SYS_QUERY_HISTORY").
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnRows(sqlmock.NewRows([]string{"query_id", "end_time", "query_text", "user_id"}).
						AddRow(1, end, `select 1\nfrom t`, 100).
						AddRow(2, end, first, 100).
						AddRow(3, end, reconFirst, 100))
			})

			logs := s.fetch(query)
			s.Require().Len(logs, 3)
			s.Equal("select 1\nfrom t", logs[0].SQL)
			s.False(logs[0].IsTruncated)
			s.Empty(logs[0].DwhContext.User)
			s.True(logs[1].IsTruncated)
			s.Equal(reconFirst, logs[2].SQL)
			s.True(logs[2].IsTruncated, "a first chunk that ends inside a quote is cut")
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
