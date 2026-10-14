package snowflake

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

// QUERY_TEXT is kept whole while its snappy encoding fits in 100 000 bytes, and a text over that
// is cut to a prefix in the band snowflakeQueryTextMayBeTruncated reports. INFORMATION_SCHEMA
// shows the same text ACCOUNT_USAGE does an hour later, without the wait.
func TestSnowflakeQueryTextTruncation(t *testing.T) {
	skipWithoutSnowflake(t)
	ctx := context.Background()
	sc, err := NewSnowflakeScrapper(ctx, &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer sc.Close()

	// SQL-like text that compresses about as well as a dbt build does.
	rnd := rand.New(rand.NewSource(1))
	var body strings.Builder
	for i := 0; body.Len() < 1_500_000; i++ {
		fmt.Fprintf(&body, "coalesce(src_%d.col_%d, lkp_%d.val_%d) as derived_%d,\n", i%37, rnd.Intn(1_000_000), i%11, rnd.Intn(1_000_000), i)
	}

	run := time.Now().UnixNano()
	statement := func(name string, snappyLen int) string {
		head := fmt.Sprintf("SELECT '%s' AS probe_%d /* ", name, run)
		return prefixWithSnappyLen(head+body.String(), snappyLen-len(" */")) + " */"
	}
	cases := map[string]struct {
		sql     string
		whole   bool
		flagged bool
	}{
		"below_band": {sql: statement("below_band", 85_000), whole: true},
		// The band a cut lands in reaches up to the limit, so a whole text at the limit cannot be
		// told from a cut one.
		"at_limit":   {sql: statement("at_limit", 100_000), whole: true, flagged: true},
		"over_limit": {sql: statement("over_limit", 100_010), flagged: true},
		"far_over":   {sql: statement("far_over", 400_000), flagged: true},
		"repetitive": {sql: fmt.Sprintf("SELECT 'repetitive' AS probe_%d /* %s */", run, strings.Repeat("x", 1_500_000)), whole: true},
	}
	for _, c := range cases {
		require.NoError(t, sc.Executor().Exec(ctx, c.sql))
	}

	var rows []struct {
		Name      string `db:"NAME"`
		QueryText string `db:"QUERY_TEXT"`
	}
	require.NoError(t, sc.Executor().Select(ctx, &rows, fmt.Sprintf(
		`SELECT split_part(query_text, '''', 2) AS name, query_text FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 100))
		WHERE query_text LIKE 'SELECT ''%%probe_%d%%'`, run)))
	require.Len(t, rows, len(cases))

	for _, row := range rows {
		c := cases[row.Name]
		require.Equal(t, c.whole, row.QueryText == c.sql, "%s: sent %d bytes, got %d", row.Name, len(c.sql), len(row.QueryText))
		require.True(t, strings.HasPrefix(c.sql, row.QueryText), row.Name)
		require.Equal(t, c.flagged, snowflakeQueryTextMayBeTruncated(row.QueryText), row.Name)
	}
}

// prefixWithSnappyLen returns the longest prefix of s whose snappy encoding takes at most n bytes.
func prefixWithSnappyLen(s string, n int) string {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if len(snappy.Encode(nil, []byte(s[:mid]))) <= n {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return s[:lo]
}
