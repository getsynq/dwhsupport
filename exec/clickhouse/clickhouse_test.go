package clickhouse

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/stretchr/testify/suite"
)

type ClickhouseSuite struct {
	suite.Suite
}

func TestClickhouseSuite(t *testing.T) {

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
		Host:            "nogwv291ou.europe-west4.gcp.clickhouse.cloud:9440",
		Username:        "default",
		Password:        "McDp5Av8VDY~i",
		DefaultDatabase: "default",
	})
	s.NoError(err)
	s.NotNil(execer)
	defer execer.Close()

	q := NewQuerier[res](execer)
	res, err := q.QueryMany(ctx, "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_catalog = ?", exec.WithArgs[res]("system"))
	s.Require().NoError(err)
	s.Require().NotEmpty(res)

}
