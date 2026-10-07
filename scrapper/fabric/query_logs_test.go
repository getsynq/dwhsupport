package fabric

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestConvertFabricRowCarriesTheLogin converts a queryinsights row as a service
// principal leaves it (anonymised): its login_name is "<appId>@<tenantId>", and
// that is who ran the query.
func TestConvertFabricRowCarriesTheLogin(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	const servicePrincipal = "0b6f0c1e-2d3a-4b5c-8d9e-0f1a2b3c4d5e@7c8d9e0f-1a2b-4c3d-9e8f-6a5b4c3d2e1f"
	submit := time.Date(2026, 10, 5, 7, 41, 38, 306667000, time.UTC)
	start := time.Date(2026, 10, 5, 7, 41, 39, 843885000, time.UTC)
	end := time.Date(2026, 10, 5, 7, 41, 39, 906543000, time.UTC)
	cases := []struct {
		name  string
		login *string
		want  string
	}{
		{"service principal", ptr(servicePrincipal), servicePrincipal},
		{"user principal name", ptr("analyst@example.com"), "analyst@example.com"},
		{"padded", ptr("  analyst@example.com "), "analyst@example.com"},
		{"empty", ptr(""), ""},
		{"null", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := &FabricQueryLogSchema{
				Database:      "SALES_DW",
				QueryID:       "3F1C2A9E-5B7D-4E60-A1C2-9D8E7F6A5B4C",
				LoginName:     tc.login,
				SubmitTime:    &submit,
				StartTime:     &start,
				EndTime:       &end,
				ElapsedMs:     ptr(int64(1600)),
				RowCount:      ptr(int64(0)),
				Status:        "Succeeded",
				StatementType: ptr("SELECT"),
				Command:       ptr("SELECT TOP 10 * FROM dbo.orders"),
			}
			log, err := convertFabricRowToQueryLog(row, obfuscator, "fabric", "abc-xyz.datawarehouse.fabric.microsoft.com")
			require.NoError(t, err)
			require.Equal(t, &querylogs.DwhContext{
				Instance: "abc-xyz.datawarehouse.fabric.microsoft.com",
				Database: "SALES_DW",
				User:     tc.want,
			}, log.DwhContext)
		})
	}
}

func ptr[T any](v T) *T { return &v }

// TestIsQueryInsightsMissing pins the exact-match-then-skip behaviour: only the
// "Invalid object name" error for the queryinsights view (raised for Lakehouse
// SQL endpoints, SQL databases and mirrored databases that have no queryinsights
// schema) marks a database skippable. Every other error — notably permission
// denials — must propagate so real misconfigurations are not silently swallowed.
func TestIsQueryInsightsMissing(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			"invalid object name for queryinsights",
			errors.New("mssql: Invalid object name 'COALESCE_DEV_TESTING.queryinsights.exec_requests_history'."),
			true,
		},
		{
			"invalid object name for some other object",
			errors.New("mssql: Invalid object name 'MYDB.dbo.some_table'."),
			false,
		},
		{
			"permission denied on queryinsights must NOT skip",
			errors.New(
				"mssql: The SELECT permission or external policy action 'Microsoft.Sql/.../Select' was denied on the object 'exec_requests_history', database 'COALESCE_QUALITY_DWHTESTING', schema 'queryinsights'.",
			),
			false,
		},
		{
			"unrelated error",
			errors.New("mssql: Login failed"),
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isQueryInsightsMissing(tc.err); got != tc.want {
				t.Fatalf("isQueryInsightsMissing(%q) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
