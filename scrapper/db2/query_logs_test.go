package db2

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDb2QueryType(t *testing.T) {
	ptr := func(s string) *string { return &s }
	cases := []struct {
		sql, stmtType, want string
	}{
		{"SELECT * FROM T", "DML, Select (blockable)", "SELECT"},
		{"  with x as (select 1 from sysibm.sysdummy1) select * from x", "DML, Select", "SELECT"},
		{"/* synq: {} */ INSERT INTO T VALUES (1)", "DML, Insert/Update/Delete", "INSERT"},
		{"-- note\nMERGE INTO T USING S ON 1=1", "DML, Insert/Update/Delete", "MERGE"},
		{"CREATE OR REPLACE VIEW V AS SELECT 1 FROM SYSIBM.SYSDUMMY1", "DDL, (not Set Constraints)", "CREATE VIEW"},
		{"create table t (id int)", "DDL, (not Set Constraints)", "CREATE TABLE"},
		{"CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE T')", "CALL", "CALL"},
		{"", "Set environment", "Set environment"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, db2QueryType(c.sql, ptr(c.stmtType)), c.sql)
	}
}

func TestConvertDb2RowToQueryLog(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	last := time.Date(2026, 9, 23, 13, 22, 36, 0, time.UTC)
	stmtID, member, execs := int64(-883132535014185310), int64(1), int64(3)
	text := "SELECT COUNT(*) FROM DWH_SALES.ORDERS"
	log, err := convertDb2RowToQueryLog(&Db2QueryLogSchema{
		ExecutableId:      "0100000000000000B6",
		StmtId:            &stmtID,
		Member:            &member,
		LastMetricsUpdate: &last,
		NumExecutions:     &execs,
		DatabaseName:      "TESTDB            ",
		StmtText:          &text,
	}, obfuscator, "db2", "db2.example.com")
	require.NoError(t, err)

	assert.Equal(t, "0100000000000000B6-1", log.QueryID, "a member other than 0 is part of the id")
	assert.Equal(t, "-883132535014185310", *log.NormalizedQueryHash)
	assert.Equal(t, last, log.CreatedAt)
	assert.Equal(t, last, *log.FinishedAt)
	assert.Equal(t, "SELECT", log.QueryType)
	assert.Equal(t, "TESTDB", log.DwhContext.Database)
	assert.Equal(t, "db2.example.com", log.DwhContext.Instance)
	assert.Equal(t, int64(3), int64(log.Metadata.Fields["num_executions"].GetNumberValue()))
}

func (s *Db2ScrapperSuite) TestFetchQueryLogs() {
	marker := "query_log_probe_" + time.Now().Format("150405.000000")
	_, err := s.scrapper.RunRawQuery(s.ctx, "SELECT COUNT(*) FROM DWH_SALES.ORDERS /* "+marker+" */")
	s.Require().NoError(err)

	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)

	iter, err := s.scrapper.FetchQueryLogs(s.ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), obfuscator)
	s.Require().NoError(err)
	defer iter.Close()

	var found bool
	var logs int
	for {
		log, err := iter.Next(s.ctx)
		if err != nil {
			s.Require().ErrorIs(err, io.EOF)
			break
		}
		logs++
		s.NotEmpty(log.QueryID)
		s.Equal("db2", log.SqlDialect)
		s.Equal("TESTDB", log.DwhContext.Database)
		s.Equal("SUCCESS", log.Status)
		s.False(log.CreatedAt.IsZero())
		s.WithinDuration(time.Now(), log.CreatedAt, 2*time.Hour, "timestamps are converted to UTC")
		if strings.Contains(log.SQL, marker) {
			found = true
			s.Equal("SELECT", log.QueryType)
			s.NotNil(log.NormalizedQueryHash)
		}
	}
	s.Positive(logs)
	s.True(found, "the query just run should be in the package cache")
}
