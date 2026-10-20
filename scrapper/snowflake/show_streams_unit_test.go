package snowflake

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMapTime(t *testing.T) {
	created := time.Date(2026, 10, 20, 5, 6, 7, 0, time.FixedZone("PDT", -7*60*60))
	row := map[string]any{"created_on": created, "comment": "x", "stale_after": nil}

	got := mapTime(row, "created_on")
	require.NotNil(t, got)
	require.Equal(t, time.UTC, got.Location())
	require.True(t, got.Equal(created))

	require.Nil(t, mapTime(row, "stale_after"), "NULL")
	require.Nil(t, mapTime(row, "missing"), "absent")
	require.Nil(t, mapTime(row, "comment"), "not a time")
}
