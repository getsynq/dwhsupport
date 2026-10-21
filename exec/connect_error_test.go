package exec_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/joho/godotenv"
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

// noSuchLogin is a login no dwhtesting warehouse has. Rejecting it exercises the
// same refusal a wrong password gets, without counting failed attempts against
// a real user, which Snowflake and Oracle lock after a few.
const noSuchLogin = "synq_no_such_login_qua2650"

// A login the warehouse refuses is an AuthError, so the integration's owner is
// told to fix the credentials. Runs against the dwhtesting warehouses named in
// .env, and skips a warehouse whose connection is not configured.
func TestLoginRejectionIsAnAuthError(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("needs the dwhtesting warehouses")
	}
	_ = godotenv.Load("../.env")

	cases := []struct {
		name    string
		hostEnv string
		connect func(ctx context.Context) error
	}{
		{"postgres", "POSTGRES_HOST", func(ctx context.Context) error {
			_, err := dwhexecpostgres.NewPostgresExecutor(ctx, &dwhexecpostgres.PostgresConf{
				User: noSuchLogin, Password: "wrong", Database: os.Getenv("POSTGRES_DATABASE"),
				Host: os.Getenv("POSTGRES_HOST"), Port: testenv.EnvOrDefaultInt("POSTGRES_PORT", 5432), AllowInsecure: true,
			})
			return err
		}},
		{"mysql", "MYSQL_HOST", func(ctx context.Context) error {
			_, err := dwhexecmysql.NewMySQLExecutor(ctx, &dwhexecmysql.MySQLConf{
				User: noSuchLogin, Password: "wrong", Database: os.Getenv("MYSQL_DATABASE"),
				Host: os.Getenv("MYSQL_HOST"), Port: testenv.EnvOrDefaultInt("MYSQL_PORT", 3306), AllowInsecure: true,
			})
			return err
		}},
		{"mariadb", "MARIADB_HOST", func(ctx context.Context) error {
			_, err := dwhexecmysql.NewMySQLExecutor(ctx, &dwhexecmysql.MySQLConf{
				User: noSuchLogin, Password: "wrong", Database: os.Getenv("MARIADB_DATABASE"),
				Host: os.Getenv("MARIADB_HOST"), Port: testenv.EnvOrDefaultInt("MARIADB_PORT", 3306), AllowInsecure: true,
			})
			return err
		}},
		{"mssql", "MSSQL_HOST", func(ctx context.Context) error {
			_, err := dwhexecmssql.NewMSSQLExecutor(ctx, &dwhexecmssql.MSSQLConf{
				User: noSuchLogin, Password: "wrong", Database: os.Getenv("MSSQL_DATABASE"),
				Host: os.Getenv("MSSQL_HOST"), Port: testenv.EnvOrDefaultInt("MSSQL_PORT", 1433),
				TrustCert: true, Encrypt: testenv.EnvOrDefault("MSSQL_ENCRYPT", "disable"),
			})
			return err
		}},
		{"oracle", "ORACLE_HOST", func(ctx context.Context) error {
			_, err := dwhexecoracle.NewOracleExecutor(ctx, &dwhexecoracle.OracleConf{
				User: noSuchLogin, Password: "wrong", ServiceName: os.Getenv("ORACLE_SERVICE"),
				Host: os.Getenv("ORACLE_HOST"), Port: testenv.EnvOrDefaultInt("ORACLE_PORT", 1521),
			})
			return err
		}},
		{"db2", "DB2_HOSTNAME", func(ctx context.Context) error {
			_, err := dwhexecdb2.NewDb2Executor(ctx, &dwhexecdb2.Db2Conf{
				User: noSuchLogin, Password: "wrong", Database: os.Getenv("DB2_DATABASE"),
				Hostname: os.Getenv("DB2_HOSTNAME"), Port: testenv.EnvOrDefaultInt("DB2_PORT", 50000),
			})
			return err
		}},
		{"clickhouse", "CLICKHOUSE_HOST", func(ctx context.Context) error {
			_, err := dwhexecclickhouse.NewClickhouseExecutor(ctx, &dwhexecclickhouse.ClickhouseConf{
				Username: noSuchLogin, Password: "wrong",
				Hostname: os.Getenv("CLICKHOUSE_HOST"), Port: testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
				NoSsl: testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
			})
			return err
		}},
		{"snowflake", "SNOWFLAKE_ACCOUNT", func(ctx context.Context) error {
			_, err := dwhexecsnowflake.NewSnowflakeExecutor(ctx, &dwhexecsnowflake.SnowflakeConf{
				User: noSuchLogin, Password: "wrong", Account: os.Getenv("SNOWFLAKE_ACCOUNT"),
			})
			return err
		}},
		{"athena", "ATHENA_REGION", func(ctx context.Context) error {
			_, err := dwhexecathena.NewAthenaExecutor(ctx, &dwhexecathena.AthenaConf{
				Region: os.Getenv("ATHENA_REGION"), AccessKeyID: "AKIAQUA2650NOSUCHKEY", SecretAccessKey: "wrong",
			})
			return err
		}},
		{"bigquery with a token the API refuses", "BIGQUERY_PROJECT_ID", func(ctx context.Context) error {
			_, err := dwhexecbigquery.NewBigqueryExecutor(ctx, &dwhexecbigquery.BigQueryConf{
				ProjectId: os.Getenv("BIGQUERY_PROJECT_ID"), AccessToken: "ya29.qua2650-not-a-token",
			})
			return err
		}},
		{"bigquery with a key the token endpoint refuses", "BIGQUERY_PROJECT_ID", func(ctx context.Context) error {
			_, err := dwhexecbigquery.NewBigqueryExecutor(ctx, &dwhexecbigquery.BigQueryConf{
				ProjectId: os.Getenv("BIGQUERY_PROJECT_ID"), CredentialsJson: unknownServiceAccountKey(t),
			})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getenv(tc.hostEnv) == "" {
				t.Skipf("%s is not set", tc.hostEnv)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			err := tc.connect(ctx)
			require.Error(t, err)
			var authErr *exec.AuthError
			require.True(t, errors.As(err, &authErr), "a refused login is not reported as an AuthError: %v", err)
		})
	}
}

// unknownServiceAccountKey is a service account key Google has never issued: a
// fresh RSA key under a made-up account, so the token endpoint refuses the JWT
// it signs.
func unknownServiceAccountKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	project := os.Getenv("BIGQUERY_PROJECT_ID")
	credentials, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     project,
		"private_key_id": "0000000000000000000000000000000000000000",
		"private_key":    string(keyPem),
		"client_email":   fmt.Sprintf("%s@%s.iam.gserviceaccount.com", "qua2650-no-such-account", project),
		"client_id":      "000000000000000000000",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	require.NoError(t, err)
	return string(credentials)
}
