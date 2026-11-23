package postgres

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/require"
)

// TestPostgresStringLiteralRoundTrip sends values through StringLiteral in both
// standard_conforming_strings modes and reads them back byte for byte. The
// setting decides whether a backslash escapes inside a plain literal.
func TestPostgresStringLiteralRoundTrip(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres string literal tests in CI")
	}
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		t.Skip("POSTGRES_HOST env var not set")
	}
	ctx := context.Background()
	sc, err := newPostgresScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to Postgres: %v", err)
	}
	defer func() { _ = sc.Close() }()

	// one connection, so the SET and the statements share a session
	conn, err := sc.Executor().GetDb().Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	d := sqldialect.NewPostgresDialect()
	values := []string{`a\b`, `\'`, `x\`, `'`, `\'); DROP TABLE t; --`}

	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "SET standard_conforming_strings = "+mode)
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "RESET standard_conforming_strings") }()
			_, err = conn.ExecContext(ctx, "CREATE TEMP TABLE t (id int, v text)")
			require.NoError(t, err)
			defer func() { _, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS pg_temp.t") }()

			for i, v := range values {
				_, err := conn.ExecContext(ctx, "INSERT INTO t VALUES ("+strconv.Itoa(i)+", "+d.StringLiteral(v)+")")
				require.NoError(t, err, "value %q", v)
			}
			for i, v := range values {
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT v FROM t WHERE id = "+strconv.Itoa(i)).Scan(&got))
				require.Equal(t, v, got)
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT "+d.StringLiteral(v)).Scan(&got))
				require.Equal(t, v, got)
			}
		})
	}
}
