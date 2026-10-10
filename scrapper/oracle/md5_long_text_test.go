package oracle

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/jmoiron/sqlx"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

// md5OfConcatWs is the digest under test: the MD5 of a ConcatWs, as a RAW(16).
func md5OfConcatWs(text sqldialect.Expr) sqldialect.Expr {
	return sqldialect.WrapSql("STANDARD_HASH(%s, 'MD5')", text)
}

// connectAsSys connects as SYS, which owns DBMS_CRYPTO, so the long text
// digest runs without a grant the test user does not have.
func connectAsSys(t *testing.T) *sqlx.DB {
	t.Helper()
	connStr := go_ora.BuildUrl(
		testenv.EnvOrDefault("ORACLE_HOST", "127.0.0.1"),
		testenv.EnvOrDefaultInt("ORACLE_PORT", 1521),
		testenv.EnvOrDefault("ORACLE_SERVICE", "FREEPDB1"),
		"sys",
		testenv.EnvOrDefault("ORACLE_SYS_PASSWORD", "SynqTest1"),
		map[string]string{"dba privilege": "sysdba"},
	)
	db, err := sqlx.Open("oracle", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("Could not connect to Oracle as SYS: %v", err)
	}
	return db
}

// TestOracleMd5OfConcatWs hashes the text of a ConcatWs on Oracle and expects
// the MD5 of its UTF-8 bytes, the digest every other engine computes for the
// same values. A VARCHAR2 holds 4000 bytes, so a wide row or a few long values
// make a text the concatenation alone cannot build.
func TestOracleMd5OfConcatWs(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle tests in CI")
	}
	db := connectAsSys(t)

	type part struct {
		expr sqldialect.Expr
		text string
	}
	str := func(s string) part { return part{sqldialect.String(s), s} }
	rpad := func(c string, n int) part {
		return part{
			sqldialect.WrapSql("RPAD(%s, %s, %s)", sqldialect.String(c), sqldialect.Int64(int64(n)), sqldialect.String(c)),
			strings.Repeat(c, n),
		}
	}
	null := part{sqldialect.Sql("NULL"), ""}
	repeat := func(p part, n int) []part {
		parts := make([]part, n)
		for i := range parts {
			parts[i] = p
		}
		return parts
	}

	cases := []struct {
		name  string
		parts []part
	}{
		{"short", []part{str("1"), str("zażółć"), str("<NULL>")}},
		{"short with a NULL", []part{str("a"), null, str("b")}},
		{"exactly 4000 bytes", []part{rpad("a", 1999), rpad("b", 2000)}},
		{"4001 bytes", []part{rpad("a", 2000), rpad("b", 2000)}},
		{"wide row", repeat(str("1.000000"), 900)},
		{"long values", []part{rpad("x", 3000), rpad("ż", 1500), str("tail")}},
		{"long values with a NULL", []part{rpad("x", 3000), null, rpad("y", 3000)}},
	}
	dialect := sqldialect.NewOracleDialect()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exprs := make([]sqldialect.Expr, len(tc.parts))
			texts := make([]string, len(tc.parts))
			for i, p := range tc.parts {
				exprs[i] = p.expr
				texts[i] = p.text
			}
			digest, err := md5OfConcatWs(sqldialect.ConcatWs("|", exprs...)).ToSql(dialect)
			require.NoError(t, err)

			var got string
			require.NoError(t, db.GetContext(context.Background(), &got, "SELECT RAWTOHEX("+digest+") FROM dual"))
			want := md5.Sum([]byte(strings.Join(texts, "|")))
			require.Equal(t, strings.ToUpper(hex.EncodeToString(want[:])), got)
		})
	}
}
