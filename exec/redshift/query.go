package redshift

import (
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

func NewQuerier[T any](conn *RedshiftExecutor) querier.Querier[T] {
	return stdsql.NewQuerier(conn.db, stdsql.Querier_WithSshTunnelDialer[T](conn.sshTunnelDialer))
}
