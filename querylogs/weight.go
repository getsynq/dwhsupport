package querylogs

import (
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// QueryWeight is what one query cost the warehouse, in the platform's own figures. A nil figure
// is one the platform did not report; a zero is a value. Nothing here is a credit or a price:
// turning a weight into either needs the platform's metering, which a query log does not hold.
type QueryWeight struct {
	// ExecutionMs is how long the query held compute, in milliseconds. Queueing and compilation
	// are left out wherever the platform reports them apart.
	ExecutionMs *int64

	// ComputeMs is the platform's own unit of work, in milliseconds of it. It is comparable only
	// within one platform: Snowflake reports X-Small-warehouse milliseconds, BigQuery slot
	// milliseconds and Databricks task milliseconds.
	ComputeMs *int64

	// BytesScanned is how many bytes the query read.
	BytesScanned *int64

	// ComputeId names the compute the query ran on: the Snowflake warehouse id, the BigQuery
	// reservation (the project when it ran on none) or the Databricks SQL warehouse. Empty where
	// the platform has none.
	ComputeId string
}

// IsEmpty reports whether the platform reported nothing to weigh the query by.
func (w QueryWeight) IsEmpty() bool {
	return w.ExecutionMs == nil && w.ComputeMs == nil && w.BytesScanned == nil && w.ComputeId == ""
}

// Weight decodes what the query cost from its metadata. See DecodeQueryWeight.
func (q *QueryLog) Weight() QueryWeight {
	if q == nil {
		return QueryWeight{}
	}
	return DecodeQueryWeight(q.SqlDialect, q.Metadata)
}

// DecodeQueryWeight reads a query's weight from the metadata a platform's FetchQueryLogs wrote, for
// the dialect it reported (Scrapper.DialectType()). It reads metadata only, so a caller holding a
// stored query log decodes it the same way the fetch would, whenever it was fetched.
//
//   - snowflake: EXECUTION_TIME; EXECUTION_TIME × the credits per hour of WAREHOUSE_SIZE ×
//     QUERY_LOAD_PERCENT, only where Snowflake reported both; BYTES_SCANNED; WAREHOUSE_ID.
//   - bigquery: end_time − start_time; total_slot_ms; total_bytes_processed; reservation_id, else
//     project_id. A SCRIPT job gets no figures, since its own are the sum of its child jobs'.
//   - databricks: execution_time_ms; task_total_time_ms; read_bytes; warehouse_id, else
//     endpoint_id.
//   - redshift: SYS_QUERY_HISTORY execution_time, which is zero for a statement the leader node
//     answered alone. Bytes need SYS_QUERY_DETAIL, which the fetch does not read.
//   - clickhouse: query_duration_ms; read_bytes.
//
// Every other dialect, and a nil metadata, has no weight.
func DecodeQueryWeight(sqlDialect string, metadata *structpb.Struct) QueryWeight {
	fields := metadata.GetFields()
	if len(fields) == 0 {
		return QueryWeight{}
	}
	switch sqlDialect {
	case "snowflake":
		return snowflakeWeight(fields)
	case "bigquery":
		return bigqueryWeight(fields)
	case "databricks":
		return databricksWeight(fields)
	case "redshift":
		return QueryWeight{ExecutionMs: microsToMillis(numberField(fields, "execution_time"))}
	case "clickhouse":
		return QueryWeight{
			ExecutionMs:  numberField(fields, "query_duration_ms"),
			BytesScanned: numberField(fields, "read_bytes"),
		}
	}
	return QueryWeight{}
}

// snowflakeCreditsPerHour is what a standard warehouse of each WAREHOUSE_SIZE bills per hour, which
// makes an X-Small the unit. Another warehouse type bills a multiple of the same table, which
// cancels out between the queries of one warehouse. ADAPTIVE has no size to multiply by.
var snowflakeCreditsPerHour = map[string]int64{
	"xsmall":  1,
	"small":   2,
	"medium":  4,
	"large":   8,
	"xlarge":  16,
	"2xlarge": 32,
	"3xlarge": 64,
	"4xlarge": 128,
	"5xlarge": 256,
	"6xlarge": 512,
}

func snowflakeWeight(fields map[string]*structpb.Value) QueryWeight {
	execution := numberField(fields, "execution_time")
	weight := QueryWeight{
		ExecutionMs:  execution,
		BytesScanned: numberField(fields, "bytes_scanned"),
	}
	if id := numberField(fields, "warehouse_id"); id != nil {
		weight.ComputeId = strconv.FormatInt(*id, 10)
	}
	// QUERY_LOAD_PERCENT is left empty on a query that took no measurable share of the warehouse:
	// a dynamic table refresh that found nothing to do is most of them, and an hour holding only
	// those meters no compute. Reading empty as 100 would charge them a whole warehouse.
	credits, sized := snowflakeCreditsPerHour[normalizeWarehouseSize(stringField(fields, "warehouse_size"))]
	load := numberField(fields, "query_load_percent")
	if execution != nil && sized && load != nil {
		computeMs := int64(math.Round(float64(*execution) * float64(credits) * float64(*load) / 100))
		weight.ComputeMs = &computeMs
	}
	return weight
}

func normalizeWarehouseSize(size string) string {
	return strings.NewReplacer("-", "", "_", "", " ", "").Replace(strings.ToLower(size))
}

func bigqueryWeight(fields map[string]*structpb.Value) QueryWeight {
	weight := QueryWeight{ComputeId: stringField(fields, "reservation_id")}
	if weight.ComputeId == "" {
		weight.ComputeId = stringField(fields, "project_id")
	}
	if stringField(fields, "statement_type") == "SCRIPT" {
		return weight
	}
	weight.ComputeMs = numberField(fields, "total_slot_ms")
	weight.BytesScanned = numberField(fields, "total_bytes_processed")
	start, startErr := time.Parse(time.RFC3339Nano, stringField(fields, "start_time"))
	end, endErr := time.Parse(time.RFC3339Nano, stringField(fields, "end_time"))
	if startErr == nil && endErr == nil && !end.Before(start) {
		elapsed := end.Sub(start).Milliseconds()
		weight.ExecutionMs = &elapsed
	}
	return weight
}

func databricksWeight(fields map[string]*structpb.Value) QueryWeight {
	weight := QueryWeight{ComputeId: stringField(fields, "warehouse_id")}
	if weight.ComputeId == "" {
		weight.ComputeId = stringField(fields, "endpoint_id")
	}
	metricsValue, ok := fields["metrics"]
	if !ok {
		return weight
	}
	metrics := metricsValue.GetStructValue().GetFields()
	// The fetch once wrote a metric only when it was not zero, so inside the metrics an absent
	// metric is a zero.
	metric := func(key string) *int64 {
		if v := numberField(metrics, key); v != nil {
			return v
		}
		zero := int64(0)
		return &zero
	}
	weight.ExecutionMs = metric("execution_time_ms")
	weight.ComputeMs = metric("task_total_time_ms")
	weight.BytesScanned = metric("read_bytes")
	return weight
}

// numberField reads a whole number, written either as a number or as its decimal text.
func numberField(fields map[string]*structpb.Value, key string) *int64 {
	v, ok := fields[key]
	if !ok {
		return nil
	}
	switch kind := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		if math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) {
			return nil
		}
		n := int64(math.Round(kind.NumberValue))
		return &n
	case *structpb.Value_StringValue:
		n, err := strconv.ParseInt(strings.TrimSpace(kind.StringValue), 10, 64)
		if err != nil {
			return nil
		}
		return &n
	}
	return nil
}

func stringField(fields map[string]*structpb.Value, key string) string {
	return strings.TrimSpace(fields[key].GetStringValue())
}

func microsToMillis(micros *int64) *int64 {
	if micros == nil {
		return nil
	}
	millis := int64(math.Round(float64(*micros) / 1000))
	return &millis
}
