package trino

import (
	"context"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type QuerySqlDefinitionsSuite struct {
	suite.Suite
}

func TestQuerySqlDefinitionsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(QuerySqlDefinitionsSuite))
}

func (s *QuerySqlDefinitionsSuite) TestQuerySqlDefinitions() {
	// please run locally to make test pass:
	// docker run --name trino -d -p 8080:8080 trinodb/trino
	// or run bash or docker compose up in cloud/def-infra/trino repository/directory
	ctx := context.TODO()
	conf := &trino.TrinoConf{
		Host:     "localhost",
		Port:     8080,
		User:     "trino",
		Password: "trino",
	}
	scr, err := NewTrinoScrapper(ctx, &TrinoScrapperConf{
		TrinoConf: conf,
		Catalogs:  []string{"iceberg"},
	})
	s.Require().NoError(err)
	s.Require().NotNil(scr)
	defer scr.Close()

	rows, err := scr.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	spew.Dump(rows)

	// Spot check first row fields
	row := rows[0]
	s.Equal("localhost", row.Instance)
	s.Equal("iceberg", row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotEmpty(row.Sql)
	s.True(row.IsView)
}
