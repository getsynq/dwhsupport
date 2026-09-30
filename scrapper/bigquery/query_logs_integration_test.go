package bigquery

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/suite"
)

// BigQueryQueryLogsSuite runs statements on a real project and reads them back through
// FetchQueryLogs, to check what INFORMATION_SCHEMA.JOBS reports for them.
type BigQueryQueryLogsSuite struct {
	suite.Suite
	scrapper *BigQueryScrapper
}

func TestBigQueryQueryLogsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery query log tests in CI")
	}
	suite.Run(t, new(BigQueryQueryLogsSuite))
}

func (s *BigQueryQueryLogsSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.scrapper = sc
}

func (s *BigQueryQueryLogsSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func (s *BigQueryQueryLogsSuite) run(statement string) {
	rows, err := s.scrapper.RunRawQuery(context.Background(), statement)
	s.Require().NoError(err)
	for {
		_, err := rows.Next(context.Background())
		if err == io.EOF {
			break
		}
		s.Require().NoError(err)
	}
	s.Require().NoError(rows.Close())
}

// logsBySQL reads the query logs since from until every statement holding marker has reached
// INFORMATION_SCHEMA.JOBS, which lags the job by a few seconds.
func (s *BigQueryQueryLogsSuite) logsBySQL(from time.Time, marker string, wantCount int) map[string]*querylogs.QueryLog {
	ctx := context.Background()
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)

	found := map[string]*querylogs.QueryLog{}
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		it, err := s.scrapper.FetchQueryLogs(ctx, from, time.Now().UTC().Add(time.Minute), obfuscator)
		s.Require().NoError(err)
		found = map[string]*querylogs.QueryLog{}
		for {
			log, err := it.Next(ctx)
			if err == io.EOF {
				break
			}
			s.Require().NoError(err)
			if strings.Contains(log.SQL, marker) && !strings.Contains(log.SQL, "INFORMATION_SCHEMA") {
				found[log.SQL] = log
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

func (s *BigQueryQueryLogsSuite) TestNormalizedQueryHashIsTheShape() {
	from := time.Now().UTC().Add(-time.Minute)
	marker := fmt.Sprintf("qlog_probe_%d", time.Now().UnixNano())
	// A statement that reads no table runs in the US multi-region, which the configured region's
	// JOBS view does not list, so each one reads a fixture table.
	table := "`" + s.scrapper.conf.ProjectId + "." + datasetA() + ".products`"

	base := fmt.Sprintf("SELECT COUNT(*) AS %s_a FROM %s WHERE 5 = 5", marker, table)
	otherLiteral := fmt.Sprintf("SELECT COUNT(*) AS %s_a FROM %s WHERE 6 = 6", marker, table)
	withComment := fmt.Sprintf("SELECT COUNT(*) AS %s_a -- a comment\nFROM %s WHERE 5 = 5", marker, table)
	otherColumn := fmt.Sprintf("SELECT COUNT(*) AS %s_b FROM %s WHERE 5 = 5", marker, table)
	for _, statement := range []string{base, otherLiteral, withComment, otherColumn} {
		s.run(statement)
	}

	logs := s.logsBySQL(from, marker, 4)
	for _, statement := range []string{base, otherLiteral, withComment, otherColumn} {
		s.Require().Contains(logs, statement)
		s.Require().NotNil(logs[statement].NormalizedQueryHash, statement)
		s.T().Logf("%s -> %s", strings.ReplaceAll(statement, "\n", `\n`), *logs[statement].NormalizedQueryHash)
	}
	hash := func(statement string) string { return *logs[statement].NormalizedQueryHash }

	s.Equal(hash(base), hash(otherLiteral), "a different literal is the same shape")
	s.Equal(hash(base), hash(withComment), "a comment is the same shape")
	s.NotEqual(hash(base), hash(otherColumn), "a different column name is another shape")
	s.NotEqual(logs[base].QueryID, logs[otherLiteral].QueryID, "each job keeps its own id")
}
