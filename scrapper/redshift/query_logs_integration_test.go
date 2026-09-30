package redshift

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/suite"
)

// RedshiftQueryLogsSuite runs statements on a real Redshift and reads them back through
// FetchQueryLogs: the text as it ran, the run as its query_id, and the user who ran it.
type RedshiftQueryLogsSuite struct {
	suite.Suite
	scrapper *RedshiftScrapper
}

func TestRedshiftQueryLogsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift query log tests in CI")
	}
	suite.Run(t, new(RedshiftQueryLogsSuite))
}

func (s *RedshiftQueryLogsSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)

	sc, err := NewRedshiftScrapper(context.Background(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.scrapper = sc
}

func (s *RedshiftQueryLogsSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

// logsContaining reads the query logs since from until every one of wantCount statements holding
// marker has reached SYS_QUERY_HISTORY, which lags the statements by a few seconds or more.
func (s *RedshiftQueryLogsSuite) logsContaining(from time.Time, marker string, wantCount int) []*querylogs.QueryLog {
	ctx := context.Background()
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)

	var found []*querylogs.QueryLog
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		it, err := s.scrapper.FetchQueryLogs(ctx, from, time.Now().Add(time.Minute), obfuscator)
		s.Require().NoError(err)
		found = nil
		for {
			log, err := it.Next(ctx)
			if err == io.EOF {
				break
			}
			s.Require().NoError(err)
			if strings.Contains(log.SQL, marker) {
				found = append(found, log)
			}
		}
		if len(found) >= wantCount {
			return found
		}
		time.Sleep(10 * time.Second)
	}
	s.Require().Failf("query logs not found", "found %d of %d statements marked %s", len(found), wantCount, marker)
	return nil
}

func (s *RedshiftQueryLogsSuite) run(statement string) {
	s.Require().NoError(s.scrapper.Executor().Exec(context.Background(), statement))
}

func (s *RedshiftQueryLogsSuite) currentUser() string {
	var user string
	s.Require().NoError(s.scrapper.Executor().GetDb().Get(&user, "SELECT current_user"))
	return strings.TrimSpace(user)
}

func (s *RedshiftQueryLogsSuite) TestQueryLogsCarryTheStatementAsItRan() {
	from := time.Now().Add(-time.Minute)
	marker := fmt.Sprintf("qlog_probe_%d", time.Now().UnixNano())

	multiLine := fmt.Sprintf("select 1 as %s_multi, -- a comment to the end of the line\n\t'back\\\\slash' as b,\r\n  2 as c", marker)

	var long strings.Builder
	fmt.Fprintf(&long, "select 0 as %s_long", marker)
	for i := 0; long.Len() < 3*4000; i++ {
		// spaces, a tab, a backslash and multi-byte characters, so a chunk boundary lands on each
		fmt.Fprintf(&long, ",\n  'żółć\\\\%d'   \t  as col_%d          ", i, i)
	}
	longStatement := long.String()

	repeated := fmt.Sprintf("select 1 as %s_repeated where 5 = 5", marker)

	s.run(multiLine)
	s.run(longStatement)
	s.run(repeated)
	s.run(repeated)

	logs := s.logsContaining(from, marker, 4)
	byText := map[string][]*querylogs.QueryLog{}
	for _, log := range logs {
		byText[log.SQL] = append(byText[log.SQL], log)
	}

	s.Run("a multi-line statement keeps its line breaks, comment and backslashes", func() {
		s.Require().Len(byText[multiLine], 1)
		s.False(byText[multiLine][0].IsTruncated)
	})

	s.Run("a statement longer than one chunk is rebuilt whole", func() {
		s.Require().Len(byText[strings.TrimSpace(longStatement)], 1, "no log matched the long statement exactly")
		s.False(byText[strings.TrimSpace(longStatement)][0].IsTruncated)
	})

	s.Run("each run has its own query_id and the shape is the normalized hash", func() {
		runs := byText[repeated]
		s.Require().Len(runs, 2)
		s.NotEqual(runs[0].QueryID, runs[1].QueryID)
		s.Require().NotNil(runs[0].NormalizedQueryHash)
		s.Require().NotNil(runs[1].NormalizedQueryHash)
		s.Equal(*runs[0].NormalizedQueryHash, *runs[1].NormalizedQueryHash)
	})

	s.Run("the user who ran it is named", func() {
		user := s.currentUser()
		for _, log := range logs {
			s.Equal(user, log.DwhContext.User)
		}
	})

	s.Run("without SYS_QUERY_TEXT and pg_user the history is still read", func() {
		refuseJoins := func(ctx context.Context, sql string, args ...any) (*sqlx.Rows, error) {
			if strings.Contains(sql, "SYS_QUERY_TEXT") {
				return nil, &pq.Error{Code: "42501", Message: "permission denied for relation sys_query_text"}
			}
			return s.scrapper.Executor().QueryRows(ctx, sql, args...)
		}
		obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
		s.Require().NoError(err)
		it, err := fetchQueryLogs(context.Background(), refuseJoins, from, time.Now().Add(time.Minute), obfuscator,
			s.scrapper.DialectType(), s.scrapper.conf.Host, s.scrapper.conf.Database)
		s.Require().NoError(err)

		var multi, longLog *querylogs.QueryLog
		for {
			log, err := it.Next(context.Background())
			if err == io.EOF {
				break
			}
			s.Require().NoError(err)
			switch {
			case log.SQL == multiLine:
				multi = log
			case strings.Contains(log.SQL, marker+"_long"):
				longLog = log
			}
		}
		s.Require().NotNil(multi)
		s.False(multi.IsTruncated)
		s.Empty(multi.DwhContext.User)
		s.Require().NotNil(longLog)
		s.True(longLog.IsTruncated)
	})
}
