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
		Catalogs: []string{"tpch", "tpcds"},
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
	s.True(databases["tpcds"])

	// Spot check first row fields
	row := rows[0]
	s.Empty(row.Instance)
	s.Empty(row.Description)
	s.Empty(row.Tags)
	s.NotEmpty(row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotEmpty(row.TableType)
}
