package trino

import (
	"context"
	"os"
	"strconv"
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

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func (s *TrinoSuite) TestBasicQuery() {
	// please run locally to make test pass:
	// docker run --name trino -d -p 8080:8080 trinodb/trino
	ctx := context.TODO()
	execer, err := NewTrinoExecutor(ctx, &TrinoConf{
		Host:      envOrDefault("TRINO_HOST", "localhost"),
		Port:      envOrDefaultInt("TRINO_PORT", 8080),
		User:      envOrDefault("TRINO_USER", "trino"),
		Password:  envOrDefault("TRINO_PASSWORD", "trino"),
		Plaintext: true,
	})
	s.NoError(err)
	s.NotNil(execer)
	defer execer.Close()

	q := NewQuerier[res](execer)
	results, err := q.QueryMany(ctx, "SELECT table_catalog, table_schema, table_name, table_type FROM tpch.information_schema.tables LIMIT 1;")
	s.Require().NoError(err)
	s.Require().NotEmpty(results)
	s.Equal("tpch", results[0].TableCatalog)
	s.Equal("information_schema", results[0].TableSchema)
	s.Equal("applicable_roles", results[0].TableName)
	s.Equal("BASE TABLE", results[0].TableType)
}
