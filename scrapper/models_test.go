package scrapper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUTCTimePtr(t *testing.T) {
	require.Nil(t, UTCTimePtr(nil))
	require.Nil(t, UTCTimePtr(&time.Time{}))

	warsaw := time.FixedZone("CEST", 2*60*60)
	in := time.Date(2026, 10, 20, 12, 0, 0, 0, warsaw)
	got := UTCTimePtr(&in)
	require.NotNil(t, got)
	require.Equal(t, time.UTC, got.Location())
	require.True(t, got.Equal(in))
}
