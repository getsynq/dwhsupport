package querylogs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func ms(v int64) *int64 { return &v }

func metadataOf(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return s
}

// The rows below are real query-history rows from each platform, with every name and id replaced.
func TestDecodeQueryWeight(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		metadata map[string]any
		want     QueryWeight
	}{
		{
			name:    "snowflake: a query on a sized warehouse with its load",
			dialect: "snowflake",
			metadata: map[string]any{
				"execution_time": 1567, "warehouse_size": "X-Small", "warehouse_type": "STANDARD", "query_load_percent": 100,
				"bytes_scanned": 0, "warehouse_id": 1234, "credits_used_cloud_services": 0.000391,
			},
			want: QueryWeight{ExecutionMs: ms(1567), ComputeMs: ms(1567), BytesScanned: ms(0), ComputeId: "1234"},
		},
		{
			name:    "snowflake: the size multiplies, the load divides",
			dialect: "snowflake",
			metadata: map[string]any{
				"execution_time": 2268, "warehouse_size": "Medium", "query_load_percent": 50, "bytes_scanned": 4_194_304, "warehouse_id": 77,
			},
			want: QueryWeight{ExecutionMs: ms(2268), ComputeMs: ms(4536), BytesScanned: ms(4_194_304), ComputeId: "77"},
		},
		{
			name:    "snowflake: no load percent leaves compute empty",
			dialect: "snowflake",
			metadata: map[string]any{
				"query_type": "REFRESH_DYNAMIC_TABLE_AT_REFRESH_VERSION", "execution_time": 177, "warehouse_size": "X-Small",
				"bytes_scanned": 0, "warehouse_id": 1234,
			},
			want: QueryWeight{ExecutionMs: ms(177), BytesScanned: ms(0), ComputeId: "1234"},
		},
		{
			name:    "snowflake: a query on no warehouse",
			dialect: "snowflake",
			metadata: map[string]any{
				"execution_time": 2, "bytes_scanned": 0, "warehouse_type": "STANDARD", "credits_used_cloud_services": 0.000005,
			},
			want: QueryWeight{ExecutionMs: ms(2), BytesScanned: ms(0)},
		},
		{
			name:    "snowflake: an adaptive warehouse has no size to multiply",
			dialect: "snowflake",
			metadata: map[string]any{
				"execution_time": 900, "warehouse_size": "ADAPTIVE", "warehouse_type": "ADAPTIVE", "query_load_percent": 100,
				"bytes_scanned": 10, "warehouse_id": 5,
			},
			want: QueryWeight{ExecutionMs: ms(900), BytesScanned: ms(10), ComputeId: "5"},
		},
		{
			name:     "snowflake: the largest size",
			dialect:  "snowflake",
			metadata: map[string]any{"execution_time": 10, "warehouse_size": "6X-Large", "query_load_percent": 3},
			want:     QueryWeight{ExecutionMs: ms(10), ComputeMs: ms(154)},
		},
		{
			name:    "bigquery: a query job",
			dialect: "bigquery",
			metadata: map[string]any{
				"statement_type": "SELECT", "job_type": "QUERY", "start_time": "2026-10-08T14:58:02.197Z",
				"end_time": "2026-10-08T14:58:02.887Z", "total_slot_ms": 688, "total_bytes_processed": 109557,
				"project_id": "my-project", "reservation_id": "admin-project:EU.default",
			},
			want: QueryWeight{ExecutionMs: ms(690), ComputeMs: ms(688), BytesScanned: ms(109557), ComputeId: "admin-project:EU.default"},
		},
		{
			name:    "bigquery: a cached query on demand",
			dialect: "bigquery",
			metadata: map[string]any{
				"statement_type": "SELECT", "cache_hit": true, "start_time": "2026-10-08T14:58:02Z", "end_time": "2026-10-08T14:58:02.040Z",
				"total_slot_ms": 0, "total_bytes_processed": 0, "project_id": "my-project",
			},
			want: QueryWeight{ExecutionMs: ms(40), ComputeMs: ms(0), BytesScanned: ms(0), ComputeId: "my-project"},
		},
		{
			name:    "bigquery: a job that reported no figures",
			dialect: "bigquery",
			metadata: map[string]any{
				"statement_type": "SELECT", "start_time": "2026-10-08T14:58:02Z", "end_time": "2026-10-08T14:58:03Z",
				"project_id": "my-project", "reservation_id": "",
			},
			want: QueryWeight{ExecutionMs: ms(1000), ComputeId: "my-project"},
		},
		{
			name:    "bigquery: a script's figures are its child jobs'",
			dialect: "bigquery",
			metadata: map[string]any{
				"statement_type": "SCRIPT", "start_time": "2026-10-08T14:00:00Z", "end_time": "2026-10-08T14:05:00Z",
				"total_slot_ms": 1_200_000, "total_bytes_processed": 5_000_000, "project_id": "my-project",
			},
			want: QueryWeight{ComputeId: "my-project"},
		},
		{
			name:     "bigquery: times out of order give no execution",
			dialect:  "bigquery",
			metadata: map[string]any{"start_time": "2026-10-08T14:00:01Z", "end_time": "2026-10-08T14:00:00Z", "project_id": "p"},
			want:     QueryWeight{ComputeId: "p"},
		},
		{
			name:    "databricks: a statement on a SQL warehouse",
			dialect: "databricks",
			metadata: map[string]any{
				"statement_type": "CREATE", "duration_ms": 5396, "warehouse_id": "abc123", "endpoint_id": "abc123",
				"metrics": map[string]any{"execution_time_ms": 4820, "task_total_time_ms": 15873, "read_bytes": 1_048_576, "total_time_ms": 5396},
			},
			want: QueryWeight{ExecutionMs: ms(4820), ComputeMs: ms(15873), BytesScanned: ms(1_048_576), ComputeId: "abc123"},
		},
		{
			name:    "databricks: an older fetch left the zeros out of the metrics",
			dialect: "databricks",
			metadata: map[string]any{
				"warehouse_id": "abc123",
				"metrics":      map[string]any{"execution_time_ms": 12, "result_from_cache": true, "total_time_ms": 30},
			},
			want: QueryWeight{ExecutionMs: ms(12), ComputeMs: ms(0), BytesScanned: ms(0), ComputeId: "abc123"},
		},
		{
			name:     "databricks: no metrics at all",
			dialect:  "databricks",
			metadata: map[string]any{"endpoint_id": "abc123", "duration_ms": 30},
			want:     QueryWeight{ComputeId: "abc123"},
		},
		{
			name:     "redshift: execution_time is in microseconds",
			dialect:  "redshift",
			metadata: map[string]any{"execution_time": 106226, "elapsed_time": 818637, "queue_time": 0},
			want:     QueryWeight{ExecutionMs: ms(106)},
		},
		{
			name:     "redshift: the leader node answered alone",
			dialect:  "redshift",
			metadata: map[string]any{"execution_time": 0, "elapsed_time": 902, "query_type": "UTILITY"},
			want:     QueryWeight{ExecutionMs: ms(0)},
		},
		{
			name:     "clickhouse",
			dialect:  "clickhouse",
			metadata: map[string]any{"query_duration_ms": 23, "read_bytes": 97981, "memory_usage": 4_000_000},
			want:     QueryWeight{ExecutionMs: ms(23), BytesScanned: ms(97981)},
		},
		{
			name:     "a platform without a decoding",
			dialect:  "postgres",
			metadata: map[string]any{"execution_time": 10, "read_bytes": 5},
			want:     QueryWeight{},
		},
		{
			name:     "a number written as text",
			dialect:  "snowflake",
			metadata: map[string]any{"execution_time": "1567", "warehouse_id": "1234", "bytes_scanned": "not a number"},
			want:     QueryWeight{ExecutionMs: ms(1567), ComputeId: "1234"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := metadataOf(t, tt.metadata)
			require.Equal(t, tt.want, DecodeQueryWeight(tt.dialect, metadata))

			// A stored query log is read back from its JSON, and decodes the same.
			stored, err := protojson.Marshal(metadata)
			require.NoError(t, err)
			readBack := &structpb.Struct{}
			require.NoError(t, protojson.Unmarshal(stored, readBack))
			require.Equal(t, tt.want, DecodeQueryWeight(tt.dialect, readBack))

			log := &QueryLog{SqlDialect: tt.dialect, Metadata: metadata}
			require.Equal(t, tt.want, log.Weight())
		})
	}
}

func TestDecodeQueryWeightWithoutMetadata(t *testing.T) {
	require.True(t, DecodeQueryWeight("snowflake", nil).IsEmpty())
	require.True(t, DecodeQueryWeight("snowflake", &structpb.Struct{}).IsEmpty())
	require.True(t, (*QueryLog)(nil).Weight().IsEmpty())
	require.True(t, (&QueryLog{SqlDialect: "bigquery"}).Weight().IsEmpty())
}

func TestQueryWeightIsEmpty(t *testing.T) {
	require.True(t, QueryWeight{}.IsEmpty())
	require.False(t, QueryWeight{ExecutionMs: ms(0)}.IsEmpty(), "a zero is a value")
	require.False(t, QueryWeight{ComputeId: "wh"}.IsEmpty())
}
