package snowflake

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"time"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/pkg/errors"
)

func (s *QueryLogsSuite) queryHistoryFake(queryHistory fakeRoute) *fakeSnowflake {
	queryHistory.match = sqlContains("from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY")
	return &fakeSnowflake{t: s.T(), routes: []fakeRoute{
		{
			match:   sqlContains("DESCRIBE TABLE SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"),
			columns: []string{"name"},
			rows:    [][]driver.Value{{"QUERY_ID"}, {"QUERY_TEXT"}, {"DATABASE_NAME"}, {"START_TIME"}, {"END_TIME"}},
		},
		queryHistory,
	}}
}

// A QUERY_HISTORY read that does not answer is stopped, and the error says so, rather than holding
// the warehouse until the caller gives up.
func (s *QueryLogsSuite) TestFetchQueryLogsStopsAReadThatDoesNotAnswer() {
	fake := s.queryHistoryFake(fakeRoute{hang: true})
	conf := &SnowflakeScrapperConf{SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Databases: []string{"ANALYTICS"}}}
	sc := newFakeSnowflakeScrapper(context.Background(), fake, conf)
	sc.queryHistoryTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)

	_, err = sc.FetchQueryLogs(ctx, time.Now().Add(-time.Hour), time.Now(), obfuscator)
	s.Require().Error(err)
	s.NoError(ctx.Err(), "the read was stopped by the caller's deadline, not by the fetch")
	s.False(errors.Is(err, context.Canceled))
	s.ErrorContains(err, "QUERY_HISTORY")
}

// The time limit covers the read until it answers, never the rows streamed after it.
func (s *QueryLogsSuite) TestFetchQueryLogsStreamsPastTheTimeLimit() {
	end := time.Now().UTC()
	fake := s.queryHistoryFake(fakeRoute{
		columns: []string{"QUERY_ID", "QUERY_TEXT", "DATABASE_NAME", "START_TIME", "END_TIME"},
		rows: [][]driver.Value{
			{"q1", "select 1", "ANALYTICS", end.Add(-time.Minute), end},
			{"q2", "select 2", "ANALYTICS", end.Add(-time.Minute), end},
		},
	})
	conf := &SnowflakeScrapperConf{SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Databases: []string{"ANALYTICS"}}}
	sc := newFakeSnowflakeScrapper(context.Background(), fake, conf)
	sc.queryHistoryTimeout = 50 * time.Millisecond
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)

	iterator, err := sc.FetchQueryLogs(context.Background(), end.Add(-time.Hour), end, obfuscator)
	s.Require().NoError(err)
	defer iterator.Close()
	time.Sleep(200 * time.Millisecond)

	var ids []string
	for {
		log, err := iterator.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		s.Require().NoError(err)
		ids = append(ids, log.QueryID)
	}
	s.Equal([]string{"q1", "q2"}, ids)
}

// QUERY_HISTORY is pruned on START_TIME as well, from the longest a statement can run before the
// window, so a large account reads only the micro-partitions around the window.
func (s *QueryLogsSuite) TestBuildQueryLogsSqlBoundsStartTime() {
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	sc := &SnowflakeScrapper{conf: &SnowflakeScrapperConf{}}

	sql, err := sc.buildQueryLogsSql(context.Background(), from, to, []string{"QUERY_ID"})
	s.Require().NoError(err)
	s.Contains(sql, `START_TIME >= to_timestamp_ltz('2025-10-21T12:00:00Z')`)
	s.Contains(sql, `START_TIME <= to_timestamp_ltz('2025-10-28T13:00:00Z')`)
}

// The database names are searched for anywhere in the text, rather than matched by a pattern that
// has to span the whole text.
func (s *QueryLogsSuite) TestBuildQueryLogsSqlSearchesTheTextForTheDatabases() {
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	sc := &SnowflakeScrapper{conf: &SnowflakeScrapperConf{
		SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Databases: []string{"DB1", "DB2"}},
	}}

	sql, err := sc.buildQueryLogsSql(context.Background(), from, from.Add(time.Hour), []string{"QUERY_ID"})
	s.Require().NoError(err)
	s.Contains(sql, `REGEXP_INSTR(QUERY_TEXT, '["'' \\t\\n\\r]?(DB1|DB2)["'' \\t\\n\\r]*[.]', 1, 1, 0, 'i') > 0`)
	s.False(strings.Contains(sql, `'.*["'`), sql)
}
