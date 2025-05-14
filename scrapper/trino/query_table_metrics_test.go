package trino

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type QueryTableMetricsSuite struct {
	suite.Suite
}

func TestQueryTableMetricsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(QueryTableMetricsSuite))
}

func (s *QueryTableMetricsSuite) TestQueryTableMetrics() {
	ctx := context.TODO()
	conf := &trino.TrinoConf{
		User:     os.Getenv("STARBURST_USER"),
		Password: os.Getenv("STARBURST_PASSWORD"),
		Host:     "synq-free-gcp.trino.galaxy.starburst.io",
		Port:     443,
	}
	scr, err := NewTrinoScrapper(ctx, &TrinoScrapperConf{
		TrinoConf: conf,
		Catalogs:  []string{"iceberg_gcs"},
	})
	s.Require().NoError(err)
	s.Require().NotNil(scr)
	defer scr.Close()

	rows, err := scr.QueryTableMetrics(ctx, *new(time.Time))
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	spew.Dump(rows)

	row := rows[0]
	s.Equal("synq-free-gcp.trino.galaxy.starburst.io", row.Instance)
	s.Equal("iceberg_gcs", row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotNil(row.RowCount)
}
