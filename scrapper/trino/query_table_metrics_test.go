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
		Host:      "localhost",
		Port:      8080,
		User:      "trino",
		Password:  "trino",
		Plaintext: true,
	}
	scr, err := NewTrinoScrapper(ctx, &TrinoScrapperConf{
		TrinoConf: conf,
		Catalogs:  []string{"iceberg"},
	})
	s.Require().NoError(err)
	s.Require().NotNil(scr)
	defer scr.Close()

	rows, err := scr.QueryTableMetrics(ctx, *new(time.Time))
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	spew.Dump(rows)

	row := rows[0]
	s.Equal("localhost", row.Instance)
	s.Equal("iceberg", row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotNil(row.RowCount)
}
