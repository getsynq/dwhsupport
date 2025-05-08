package trino

import (
	"context"
	"os"
	"testing"

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
		Host:     "localhost",
		Port:     8080,
		User:     "trino",
		Password: "trino",
		Catalogs: []string{"tpch"},
	}
	scr, err := NewTrinoScrapper(ctx, conf)
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
	s.True(databases["tpch"])

	// Spot check first row fields
	row := rows[0]
	// no description in default data for tables
	s.Empty(row.Description)
	// no tags at all
	s.Empty(row.Tags)
	s.Equal("localhost", row.Instance)
	s.Equal("tpch", row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotEmpty(row.TableType)
}
