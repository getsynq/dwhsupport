package bigquery

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/stretchr/testify/suite"
)

type BigquerySuite struct {
	suite.Suite
}

func TestBigquerySuite(t *testing.T) {
	if len(os.Getenv("CI")) > 0 {
		t.SkipNow()
	}

	suite.Run(t, new(BigquerySuite))
}

type paramRes struct {
	Val int64 `bigquery:"val"`
}

func (s *BigquerySuite) TestPositionalParams() {
	ctx := context.TODO()
	execer, err := NewBigqueryExecutor(ctx, &BigQueryConf{
		ProjectId:       "nifty-motif-341212",
		CredentialsFile: "../../sa.json",
	})
	s.Require().NoError(err)
	s.Require().NotNil(execer)
	defer execer.Close()

	var receivedStats *querystats.QueryStats
	ctx = querystats.WithCallback(ctx, func(stats querystats.QueryStats) {
		receivedStats = &stats
		jsonBytes, _ := json.Marshal(stats)
		logging.GetLogger(ctx).Printf("Query stats: %s", string(jsonBytes))
	})

	q := NewQuerier[paramRes](execer)
	results, err := q.QueryMany(
		ctx,
		"SELECT ? AS val",
		exec.WithArgs[paramRes](42),
	)

	s.Require().NoError(err)
	s.Require().Len(results, 1)
	s.Equal(int64(42), results[0].Val)

	s.Require().NotNil(receivedStats, "query stats callback should have been called")
	s.NotNil(receivedStats.BytesRead)
	s.NotNil(receivedStats.RowsProduced)
	s.Equal(int64(1), *receivedStats.RowsProduced)
}
