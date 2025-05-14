package trino

import (
	"context"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type QueryTablesSuite struct {
	suite.Suite
}

func TestQueryTablesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(QueryTablesSuite))
}

func (s *QueryTablesSuite) TestQueryTables() {
	// please run locally to make test pass:
	// docker run --name trino -d -p 8080:8080 trinodb/trino
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

	rows, err := scr.QueryTables(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	// get unique databases
	databases := make(map[string]bool)
	for _, row := range rows {
		databases[row.Database] = true
	}
	s.True(databases["iceberg_gcs"])
	spew.Dump(rows)

	// Spot check first row fields
	row := rows[0]
	// no description in default data for tables
	s.Empty(row.Description)
	// no tags at all
	s.Empty(row.Tags)
	s.Equal("synq-free-gcp.trino.galaxy.starburst.io", row.Instance)
	s.Equal("iceberg_gcs", row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotEmpty(row.TableType)
}
