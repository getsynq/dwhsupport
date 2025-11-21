package querylogs

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSanitizeMetadataValue_Nil(t *testing.T) {
	assert.Nil(t, SanitizeMetadataValue(nil))
}

func TestSanitizeMetadataValue_BasicTypes(t *testing.T) {
	// Booleans
	assert.Equal(t, true, SanitizeMetadataValue(true))
	assert.Equal(t, false, SanitizeMetadataValue(false))

	// Strings
	assert.Equal(t, "hello", SanitizeMetadataValue("hello"))
	assert.Equal(t, "", SanitizeMetadataValue(""))

	// Numeric types are preserved without conversion
	assert.Equal(t, 42, SanitizeMetadataValue(42))
	assert.Equal(t, int8(42), SanitizeMetadataValue(int8(42)))
	assert.Equal(t, int16(42), SanitizeMetadataValue(int16(42)))
	assert.Equal(t, int32(42), SanitizeMetadataValue(int32(42)))
	assert.Equal(t, int64(42), SanitizeMetadataValue(int64(42)))
	assert.Equal(t, uint(42), SanitizeMetadataValue(uint(42)))
	assert.Equal(t, uint8(42), SanitizeMetadataValue(uint8(42)))
	assert.Equal(t, uint16(42), SanitizeMetadataValue(uint16(42)))
	assert.Equal(t, uint32(42), SanitizeMetadataValue(uint32(42)))
	assert.Equal(t, uint64(42), SanitizeMetadataValue(uint64(42)))
	assert.Equal(t, float64(3.14), SanitizeMetadataValue(float64(3.14)))
	assert.Equal(t, float32(3.14), SanitizeMetadataValue(float32(3.14)))
}

func TestSanitizeMetadataValue_Pointers(t *testing.T) {
	// Nil pointers
	var nilInt *int
	var nilString *string
	var nilTime *time.Time
	assert.Nil(t, SanitizeMetadataValue(nilInt))
	assert.Nil(t, SanitizeMetadataValue(nilString))
	assert.Nil(t, SanitizeMetadataValue(nilTime))

	// Non-nil pointers - dereferenced
	intVal := 42
	stringVal := "hello"
	assert.Equal(t, 42, SanitizeMetadataValue(&intVal))
	assert.Equal(t, "hello", SanitizeMetadataValue(&stringVal))
}

func TestSanitizeMetadataValue_Time(t *testing.T) {
	// Non-zero time
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	result := SanitizeMetadataValue(ts)
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", result)

	// Zero time
	var zeroTime time.Time
	assert.Nil(t, SanitizeMetadataValue(zeroTime))

	// Pointer to time
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", SanitizeMetadataValue(&ts))

	// Nil pointer to time
	var nilTime *time.Time
	assert.Nil(t, SanitizeMetadataValue(nilTime))

	// Duration
	d := 1*time.Hour + 30*time.Minute
	assert.Equal(t, "1h30m0s", SanitizeMetadataValue(d))
}

func TestSanitizeMetadataValue_NetIP(t *testing.T) {
	// IPv4
	ip := net.ParseIP("192.168.1.1")
	assert.Equal(t, "192.168.1.1", SanitizeMetadataValue(ip))

	// IPv6
	ip6 := net.ParseIP("::1")
	assert.Equal(t, "::1", SanitizeMetadataValue(ip6))

	// Nil IP
	var nilIP net.IP
	assert.Nil(t, SanitizeMetadataValue(nilIP))

	// Pointer to IP
	assert.Equal(t, "192.168.1.1", SanitizeMetadataValue(&ip))

	// Nil pointer to IP
	var nilIPPtr *net.IP
	assert.Nil(t, SanitizeMetadataValue(nilIPPtr))
}

func TestSanitizeMetadataValue_Slices(t *testing.T) {
	// Typed slices become []interface{} with preserved element types
	strings := []string{"a", "b", "c"}
	result := SanitizeMetadataValue(strings)
	assert.Equal(t, []interface{}{"a", "b", "c"}, result)

	// Numeric types are preserved
	ints := []int{1, 2, 3}
	result = SanitizeMetadataValue(ints)
	assert.Equal(t, []interface{}{1, 2, 3}, result)

	floats := []float64{1.1, 2.2, 3.3}
	result = SanitizeMetadataValue(floats)
	assert.Equal(t, []interface{}{1.1, 2.2, 3.3}, result)

	// Nil slice
	var nilSlice []string
	assert.Nil(t, SanitizeMetadataValue(nilSlice))

	// Empty slice
	emptySlice := []string{}
	result = SanitizeMetadataValue(emptySlice)
	assert.Equal(t, []interface{}{}, result)
}

func TestSanitizeMetadataValue_Maps(t *testing.T) {
	m := map[string]interface{}{
		"string": "value",
		"int":    42,
		"bool":   true,
	}
	result := SanitizeMetadataValue(m).(map[string]interface{})
	assert.Equal(t, "value", result["string"])
	assert.Equal(t, 42, result["int"])
	assert.Equal(t, true, result["bool"])

	// Nil map
	var nilMap map[string]interface{}
	assert.Nil(t, SanitizeMetadataValue(nilMap))
}

func TestSanitizeMetadataValue_NestedMaps(t *testing.T) {
	// Deeply nested map with time.Time
	nested := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"value": 42,
				"time":  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}
	result := SanitizeMetadataValue(nested).(map[string]interface{})
	level1 := result["level1"].(map[string]interface{})
	level2 := level1["level2"].(map[string]interface{})
	assert.Equal(t, 42, level2["value"])
	assert.Equal(t, "2024-01-01T00:00:00Z", level2["time"])
}

func TestSanitizeMetadataValue_Structs(t *testing.T) {
	type SimpleStruct struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	// Structs use JSON round-trip, so numbers become float64
	s := SimpleStruct{Name: "test", Value: 42}
	result := SanitizeMetadataValue(s).(map[string]interface{})
	assert.Equal(t, "test", result["name"])
	assert.Equal(t, float64(42), result["value"]) // JSON round-trip converts to float64
}

func TestSanitizeMetadata(t *testing.T) {
	m := map[string]interface{}{
		"time":   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		"int":    int64(42),
		"nested": map[string]interface{}{"value": 100},
	}

	result := SanitizeMetadata(m)
	assert.Equal(t, "2024-01-01T00:00:00Z", result["time"])
	assert.Equal(t, int64(42), result["int"])
	nested := result["nested"].(map[string]interface{})
	assert.Equal(t, 100, nested["value"])

	// Nil map
	assert.Nil(t, SanitizeMetadata(nil))
}

func TestNewMetadata(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ip := net.ParseIP("192.168.1.1")

	m := NewMetadata(
		"string", "value",
		"int", 42,
		"time", ts,
		"ip", ip,
		"nil", nil,
	)

	assert.Equal(t, "value", m["string"])
	assert.Equal(t, 42, m["int"])
	assert.Equal(t, "2024-01-01T00:00:00Z", m["time"])
	assert.Equal(t, "192.168.1.1", m["ip"])
	assert.Nil(t, m["nil"])
}

func TestNewMetadata_Panics(t *testing.T) {
	// Odd number of arguments
	assert.Panics(t, func() {
		NewMetadata("key")
	})

	// Non-string key
	assert.Panics(t, func() {
		NewMetadata(123, "value")
	})
}

func TestSanitizeMetadataValue_JSONSerializable(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	ip := net.ParseIP("192.168.1.1")

	m := NewMetadata(
		"string", "value",
		"int", int64(42),
		"float", 3.14,
		"bool", true,
		"time", ts,
		"ip", ip,
		"slice", []int{1, 2, 3},
		"map", map[string]interface{}{"nested": "value"},
		"nil", nil,
	)

	// Should serialize without error
	data, err := json.Marshal(m)
	require.NoError(t, err)

	// Should deserialize back (note: JSON unmarshals numbers to float64)
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, "value", result["string"])
	assert.Equal(t, float64(42), result["int"]) // JSON unmarshal produces float64
	assert.Equal(t, 3.14, result["float"])
	assert.Equal(t, true, result["bool"])
	assert.Equal(t, "2024-01-15T10:30:00Z", result["time"])
	assert.Equal(t, "192.168.1.1", result["ip"])
}

func TestSanitizeMetadataValue_TimeNanosPreserved(t *testing.T) {
	// Test that nanoseconds are preserved in time formatting
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	result := SanitizeMetadataValue(ts)
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", result)

	// Also test with pointer
	result = SanitizeMetadataValue(&ts)
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", result)

	// Test in metadata map
	m := SanitizeMetadata(map[string]interface{}{
		"timestamp": ts,
	})
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", m["timestamp"])
}

func TestSanitizeMetadataValue_Duration(t *testing.T) {
	d := 5 * time.Second
	assert.Equal(t, "5s", SanitizeMetadataValue(d))
}

// Custom type implementing json.Marshaler
type customMarshaler struct {
	Value int
}

func (c customMarshaler) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int{"custom_value": c.Value})
}

func TestSanitizeMetadataValue_JSONMarshaler(t *testing.T) {
	cm := customMarshaler{Value: 42}
	result := SanitizeMetadataValue(cm)
	resultMap, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(42), resultMap["custom_value"])
}

// TestSanitizeMetadata_ProtoStructConversion verifies that sanitized metadata
// can be converted to protobuf Struct without errors.
// This is the actual use case that motivated the sanitization - the error was:
// "failed to create struct from map: proto: invalid type: time.Time"
func TestSanitizeMetadata_ProtoStructConversion(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	ip := net.ParseIP("192.168.1.1")
	duration := 5*time.Hour + 30*time.Minute

	// Create metadata with all the problematic types that caused the original error
	metadata := map[string]interface{}{
		"time":     ts,
		"time_ptr": &ts,
		"duration": duration,
		"ip":       ip,
		"ip_ptr":   &ip,
		"nested": map[string]interface{}{
			"inner_time": ts,
			"inner_ip":   ip,
		},
		"string":  "value",
		"int":     int64(42),
		"float":   3.14,
		"bool":    true,
		"slice":   []int{1, 2, 3},
		"nil_val": nil,
	}

	// Sanitize the metadata
	sanitized := SanitizeMetadata(metadata)
	require.NotNil(t, sanitized)

	// This is the critical test - conversion to proto.Struct must not fail
	// Before the fix, this would fail with: "proto: invalid type: time.Time"
	protoStruct, err := structpb.NewStruct(sanitized)
	require.NoError(t, err, "sanitized metadata should be convertible to proto.Struct")
	require.NotNil(t, protoStruct)

	// Verify the converted values are correct
	fields := protoStruct.GetFields()

	// Time should be converted to RFC3339Nano string (with nanoseconds)
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", fields["time"].GetStringValue())
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", fields["time_ptr"].GetStringValue())

	// Duration should be converted to string
	assert.Equal(t, "5h30m0s", fields["duration"].GetStringValue())

	// IP should be converted to string
	assert.Equal(t, "192.168.1.1", fields["ip"].GetStringValue())
	assert.Equal(t, "192.168.1.1", fields["ip_ptr"].GetStringValue())

	// Basic types should work (proto.Struct converts all numbers to float64)
	assert.Equal(t, "value", fields["string"].GetStringValue())
	assert.Equal(t, float64(42), fields["int"].GetNumberValue())
	assert.Equal(t, 3.14, fields["float"].GetNumberValue())
	assert.Equal(t, true, fields["bool"].GetBoolValue())

	// Nested struct should work
	nested := fields["nested"].GetStructValue()
	require.NotNil(t, nested)
	nestedFields := nested.GetFields()
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", nestedFields["inner_time"].GetStringValue())
	assert.Equal(t, "192.168.1.1", nestedFields["inner_ip"].GetStringValue())

	// Slice should work
	slice := fields["slice"].GetListValue()
	require.NotNil(t, slice)
	assert.Len(t, slice.GetValues(), 3)

	// Nil should work
	assert.Equal(t, structpb.NullValue_NULL_VALUE, fields["nil_val"].GetNullValue())
}

// TestSanitizeMetadata_ProtoStructConversion_ZeroTime verifies that zero time values
// are converted to nil and handled correctly by proto.Struct
func TestSanitizeMetadata_ProtoStructConversion_ZeroTime(t *testing.T) {
	var zeroTime time.Time

	metadata := map[string]interface{}{
		"zero_time": zeroTime,
	}

	sanitized := SanitizeMetadata(metadata)
	require.NotNil(t, sanitized)

	// Zero time should become nil
	assert.Nil(t, sanitized["zero_time"])

	// Should convert to proto.Struct without error
	protoStruct, err := structpb.NewStruct(sanitized)
	require.NoError(t, err)
	require.NotNil(t, protoStruct)

	// Zero time field should be null
	fields := protoStruct.GetFields()
	assert.Equal(t, structpb.NullValue_NULL_VALUE, fields["zero_time"].GetNullValue())
}
