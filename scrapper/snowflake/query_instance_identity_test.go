package snowflake

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realAllowlistPayload is SYSTEM$ALLOWLIST() as returned by a live account,
// trimmed of the long tail of OCSP responders and CRL distribution points but
// otherwise verbatim — including the entry types that look account-shaped
// (SPCS_REGISTRY_REGIONLESS, SNOWPARK_CONNECT, OPENFLOW, EXTERNAL_TELEMETRY)
// but carry a per-feature subdomain rather than an account URL.
const realAllowlistPayload = `[
 {"host":"jh35950.europe-west4.gcp.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"},
 {"host":"cuunsqr-ir70409.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT_REGIONLESS"},
 {"host":"storage.googleapis.com","port":443,"type":"STAGE"},
 {"host":"gcpeuropewest4-86em60b-stage.storage.googleapis.com","port":443,"type":"STAGE"},
 {"host":"sfc-repo.snowflakecomputing.com","port":443,"type":"SNOWSQL_REPO"},
 {"host":"client-telemetry.snowflakecomputing.com","port":443,"type":"OUT_OF_BAND_TELEMETRY"},
 {"host":"ocsp.snowflakecomputing.com","port":80,"type":"OCSP_CACHE"},
 {"host":"CUUNSQR-IR70409.registry.snowflakecomputing.com","port":443,"type":"SPCS_REGISTRY_REGIONLESS"},
 {"host":"JH35950.snowpark.amsgru.snowflakecomputing.com","port":443,"type":"SNOWPARK_CONNECT"},
 {"host":"ocsp.digicert.com","port":80,"type":"OCSP_RESPONDER"},
 {"host":"app.snowflake.com","port":443,"type":"SNOWSIGHT_DEPLOYMENT"},
 {"host":"apps-api.c1.europe-west4.gcp.app.snowflake.com","port":443,"type":"SNOWSIGHT_DEPLOYMENT"},
 {"host":"cuunsqr-ir70409.openflow.amsgru.snowflakecomputing.com","port":443,"type":"OPENFLOW"},
 {"host":"cuunsqr-ir70409.telemetry.amsgru.snowflakecomputing.com","port":443,"type":"EXTERNAL_TELEMETRY"}
]`

func TestParseAllowlistHosts(t *testing.T) {
	t.Run("keeps only the account's own hostnames", func(t *testing.T) {
		hosts, err := parseAllowlistHosts(realAllowlistPayload)
		require.NoError(t, err)
		assert.Equal(t, []scrapper.InstanceHost{
			{Host: "jh35950.europe-west4.gcp.snowflakecomputing.com", Kind: scrapper.InstanceHostRegional},
			{Host: "cuunsqr-ir70409.snowflakecomputing.com", Kind: scrapper.InstanceHostRegionless},
		}, hosts)
	})

	t.Run("returns hostnames verbatim", func(t *testing.T) {
		// The caller owns normalization: it has to reproduce the exact
		// identifier a producer would mint, which this package cannot know.
		hosts, err := parseAllowlistHosts(
			`[{"host":"XY12345.US-EAST-2.AWS.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"}]`,
		)
		require.NoError(t, err)
		require.Len(t, hosts, 1)
		assert.Equal(t, "XY12345.US-EAST-2.AWS.snowflakecomputing.com", hosts[0].Host)
	})

	t.Run("drops duplicates, keeping allowlist order", func(t *testing.T) {
		hosts, err := parseAllowlistHosts(`[
			{"host":"a.snowflakecomputing.com","type":"SNOWFLAKE_DEPLOYMENT"},
			{"host":"b.snowflakecomputing.com","type":"SNOWFLAKE_DEPLOYMENT_REGIONLESS"},
			{"host":"a.snowflakecomputing.com","type":"SNOWFLAKE_DEPLOYMENT"}
		]`)
		require.NoError(t, err)
		assert.Equal(t, []scrapper.InstanceHost{
			{Host: "a.snowflakecomputing.com", Kind: scrapper.InstanceHostRegional},
			{Host: "b.snowflakecomputing.com", Kind: scrapper.InstanceHostRegionless},
		}, hosts)
	})

	t.Run("no account hosts yields nothing rather than an error", func(t *testing.T) {
		hosts, err := parseAllowlistHosts(`[{"host":"storage.googleapis.com","port":443,"type":"STAGE"}]`)
		require.NoError(t, err)
		assert.Empty(t, hosts)
	})

	t.Run("empty payload", func(t *testing.T) {
		hosts, err := parseAllowlistHosts("   ")
		require.NoError(t, err)
		assert.Empty(t, hosts)
	})

	t.Run("malformed payload errors", func(t *testing.T) {
		_, err := parseAllowlistHosts(`not json`)
		require.Error(t, err)
	})
}
