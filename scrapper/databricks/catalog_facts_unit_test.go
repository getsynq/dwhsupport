package databricks

import (
	"encoding/json"
	"testing"
	"time"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestColumnNullable(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want *bool
	}{
		{name: "nullable", body: `{"name":"a","nullable":true}`, want: lo.ToPtr(true)},
		{name: "declared NOT NULL", body: `{"name":"a","nullable":false}`, want: lo.ToPtr(false)},
		{name: "not reported", body: `{"name":"a"}`, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var column servicecatalog.ColumnInfo
			require.NoError(t, json.Unmarshal([]byte(tc.body), &column))
			require.Equal(t, tc.want, columnNullable(column))
		})
	}
}

func TestEpochMillisToTime(t *testing.T) {
	require.Nil(t, epochMillisToTime(0))

	got := epochMillisToTime(1700000000123)
	require.NotNil(t, got)
	require.Equal(t, time.UTC, got.Location())
	require.True(t, got.Equal(time.Date(2023, 11, 14, 22, 13, 20, 123_000_000, time.UTC)), got.String())
}
