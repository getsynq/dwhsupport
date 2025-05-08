package trino

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type TrinoSuite struct {
	suite.Suite
}

func TestTrinoSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(TrinoSuite))
}

type res struct {
	TableCatalog string `db:"table_catalog"`
	TableSchema  string `db:"table_schema"`
	TableName    string `db:"table_name"`
	TableType    string `db:"table_type"`
}

func (s *TrinoSuite) TestBasicQuery() {
	// please run locally to make test pass:
	// docker run --name trino -d -p 8080:8080 trinodb/trino
	ctx := context.TODO()
	execer, err := NewTrinoExecutor(ctx, &TrinoConf{
		Host:     "localhost",
		Port:     8080,
		User:     "trino",
		Password: "trino",
	})
	s.NoError(err)
	s.NotNil(execer)
	defer execer.Close()

	q := NewQuerier[res](execer)
	results, err := q.QueryMany(ctx, "SELECT table_catalog, table_schema, table_name, table_type FROM tpch.information_schema.tables LIMIT 1")
	s.Require().NoError(err)
	s.Require().NotEmpty(results)
	s.Equal("tpch", results[0].TableCatalog)
	s.Equal("information_schema", results[0].TableSchema)
	s.Equal("applicable_roles", results[0].TableName)
	s.Equal("BASE TABLE", results[0].TableType)
}
