package clickhouse

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/stretchr/testify/suite"
)

type ClickhouseSuite struct {
	suite.Suite
}

func TestClickhouseSuite(t *testing.T) {
	if len(os.Getenv("CI")) > 0 {
		t.SkipNow()
	}

	suite.Run(t, new(ClickhouseSuite))
}

type res struct {
	TableCatalog string `db:"table_catalog"`
	TableSchema  string `db:"table_schema"`
	TableName    string `db:"table_name"`
	TableType    string `db:"table_type"`
}

func (s *ClickhouseSuite) TestSomething() {
	ctx := context.TODO()
	execer, err := NewClickhouseExecutor(ctx, &ClickhouseConf{
		Hostname:        "localhost",
		Port:            9440,
		Username:        "default",
		Password:        "",
		DefaultDatabase: "default",
	})
	s.NoError(err)
	s.NotNil(execer)
	defer execer.Close()

	q := NewQuerier[res](execer)
	res, err := q.QueryMany(
		ctx,
		"SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_catalog = ?",
		exec.WithArgs[res]("system"),
	)
	s.Require().NoError(err)
	s.Require().NotEmpty(res)

}
