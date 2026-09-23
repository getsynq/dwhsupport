package db2

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// largeInListQuery returns a query of at least minBytes whose single row
// holds how many of the IN list's values matched, which is always 1.
func largeInListQuery(minBytes int) string {
	var b strings.Builder
	b.WriteString("SELECT COUNT(*) AS N FROM SYSIBM.SYSDUMMY1 WHERE 1 IN (1")
	for i := 2; b.Len() < minBytes; i++ {
		fmt.Fprintf(&b, ", %d", i)
	}
	b.WriteString(")")
	return b.String()
}

// The DRDA request carrying the statement text has a 15-bit length field;
// statements past it have to be sent with an extended length and split over
// continued DSS segments.
func TestLargeQueries(t *testing.T) {
	skipInCI(t)
	ctx := context.Background()
	sc, err := newDb2ScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to Db2: %v", err)
	}
	defer sc.Close()

	for _, size := range []int{16 << 10, 32 << 10, 64 << 10, 128 << 10, 1 << 20} {
		query := largeInListQuery(size)
		t.Run(fmt.Sprintf("query_%dB", len(query)), func(t *testing.T) {
			var n []int64
			require.NoError(t, sc.executor.Select(ctx, &n, query))
			require.Equal(t, []int64{1}, n)
		})
		t.Run(fmt.Sprintf("query_with_arg_%dB", len(query)), func(t *testing.T) {
			var n []int64
			require.NoError(t, sc.executor.Select(ctx, &n, query+" AND 1 = ?", 1))
			require.Equal(t, []int64{1}, n)
		})
		t.Run(fmt.Sprintf("exec_%dB", len(query)), func(t *testing.T) {
			stmt := "SET CURRENT QUERY OPTIMIZATION = 5 " + strings.Repeat(" ", len(query))
			require.NoError(t, sc.executor.Exec(ctx, stmt))
		})
	}
}
