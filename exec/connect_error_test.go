package exec_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// closedPort is a local port nothing listens on, so a connection to it is
// refused at once.
func closedPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

// refusingTransport fails every request the way a host that is down does.
type refusingTransport struct{}

func (refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}
}

// A warehouse that cannot be reached is not a warehouse that refused the
// credentials. Reporting it as an AuthError tells the integration's owner to
// change credentials that are fine, so every executor's connection check has to
// leave a network failure untyped.
func TestConnectionFailureIsNotAnAuthError(t *testing.T) {
	cases := []struct {
		name    string
		connect func(ctx context.Context, port int) error
	}{
		{"postgres", func(ctx context.Context, port int) error {
			_, err := dwhexecpostgres.NewPostgresExecutor(ctx, &dwhexecpostgres.PostgresConf{
				User: "u", Password: "p", Database: "d", Host: "127.0.0.1", Port: port, AllowInsecure: true,
			})
			return err
		}},
		{"redshift", func(ctx context.Context, port int) error {
			_, err := dwhexecredshift.NewRedshiftExecutor(ctx, &dwhexecredshift.RedshiftConf{
				User: "u", Password: "p", Database: "d", Host: "127.0.0.1", Port: port,
			})
			return err
		}},
		{"mysql", func(ctx context.Context, port int) error {
			_, err := dwhexecmysql.NewMySQLExecutor(ctx, &dwhexecmysql.MySQLConf{
				User: "u", Password: "p", Database: "d", Host: "127.0.0.1", Port: port, AllowInsecure: true,
			})
			return err
		}},
		{"mssql", func(ctx context.Context, port int) error {
			_, err := dwhexecmssql.NewMSSQLExecutor(ctx, &dwhexecmssql.MSSQLConf{
				User: "u", Password: "p", Database: "d", Host: "127.0.0.1", Port: port,
			})
			return err
		}},
		{"oracle", func(ctx context.Context, port int) error {
			_, err := dwhexecoracle.NewOracleExecutor(ctx, &dwhexecoracle.OracleConf{
				User: "u", Password: "p", ServiceName: "s", Host: "127.0.0.1", Port: port,
			})
			return err
		}},
		{"db2", func(ctx context.Context, port int) error {
			_, err := dwhexecdb2.NewDb2Executor(ctx, &dwhexecdb2.Db2Conf{
				User: "u", Password: "p", Database: "d", Hostname: "127.0.0.1", Port: port,
			})
			return err
		}},
		{"clickhouse", func(ctx context.Context, port int) error {
			_, err := dwhexecclickhouse.NewClickhouseExecutor(ctx, &dwhexecclickhouse.ClickhouseConf{
				Username: "u", Password: "p", Hostname: "127.0.0.1", Port: port, NoSsl: true,
			})
			return err
		}},
		{"clickhouse with a default database", func(ctx context.Context, port int) error {
			_, err := dwhexecclickhouse.NewClickhouseExecutor(ctx, &dwhexecclickhouse.ClickhouseConf{
				Username: "u", Password: "p", Hostname: "127.0.0.1", Port: port, NoSsl: true, DefaultDatabase: "d",
			})
			return err
		}},
		{"snowflake", func(ctx context.Context, port int) error {
			_, err := dwhexecsnowflake.NewSnowflakeExecutor(ctx, &dwhexecsnowflake.SnowflakeConf{
				User: "u", Password: "p", Account: "acme", Transporter: refusingTransport{},
			})
			return err
		}},
		{"databricks", func(ctx context.Context, port int) error {
			_, err := dwhexecdatabricks.NewDatabricksExecutor(ctx, &dwhexecdatabricks.DatabricksConf{
				WorkspaceUrl: fmt.Sprintf("http://127.0.0.1:%d", port),
				Auth:         dwhexecdatabricks.NewTokenAuth("token"),
				WarehouseId:  "w",
			})
			return err
		}},
		{"athena", func(ctx context.Context, port int) error {
			_, err := dwhexecathena.NewAthenaExecutor(ctx, &dwhexecathena.AthenaConf{
				Region: "us-east-1", AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secret",
			})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port := closedPort(t)
			// The AWS SDK resolves every endpoint from this, so STS is the closed port too.
			t.Setenv("AWS_ENDPOINT_URL", fmt.Sprintf("http://127.0.0.1:%d", port))
			// Some clients retry a refused connection; the deadline bounds that, and an
			// expired deadline is no more an authentication failure than the refusal.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := tc.connect(ctx, port)
			require.Error(t, err)
			var authErr *exec.AuthError
			require.False(t, errors.As(err, &authErr), "a refused connection is reported as an AuthError: %v", err)
		})
	}
}
