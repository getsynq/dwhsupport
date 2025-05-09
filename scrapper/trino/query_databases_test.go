package trino

import (
	"context"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type QueryDatabasesSuite struct {
	suite.Suite
}

func TestQueryDatabasesSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(QueryDatabasesSuite))
}

func (s *QueryDatabasesSuite) TestQueryDatabases() {
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

	rows, err := scr.QueryDatabases(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	spew.Dump(rows)

	row := rows[0]
	s.Equal("localhost", row.Instance)
	s.NotEmpty(row.Database)
	s.NotEmpty(row.DatabaseType)
}
