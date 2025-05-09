package trino

import (
	"context"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type QueryCatalogSuite struct {
	suite.Suite
}

func TestQueryCatalogSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(QueryCatalogSuite))
}

func (s *QueryCatalogSuite) TestQueryCatalog() {
	// please run locally to make test pass:
	// docker run --name trino -d -p 8080:8080 trinodb/trino
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

	rows, err := scr.QueryCatalog(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	spew.Dump(rows)

	row := rows[0]
	s.Equal("localhost", row.Instance)
	s.Equal("iceberg", row.Database)
	s.NotEmpty(row.Schema)
	s.NotEmpty(row.Table)
	s.NotEmpty(row.Column)
	s.NotZero(row.Position)
	s.NotEmpty(row.Type)
	// table_comment and comment can be nil, just check type
	s.IsType(new(string), row.TableComment)
	s.IsType(new(string), row.Comment)

	// not supported
	s.False(row.IsStructColumn)
	s.False(row.IsArrayColumn)
	s.Empty(row.TableTags)
	s.Empty(row.ColumnTags)
}
