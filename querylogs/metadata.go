package querylogs

import (
	"encoding/json"
	"net"
	"reflect"
	"time"
)

// Metadata is a type alias for metadata maps that are guaranteed to contain only
// JSON-compatible types suitable for serialization to proto.Struct, JSON, etc.
//
// Valid value types in this map:
//   - nil
//   - bool
//   - numeric types (int, int64, float64, etc. - preserved without conversion)
//   - string (including time.Time formatted as RFC3339Nano, net.IP as string)
//   - []interface{} (slices)
//   - map[string]interface{} (nested maps)
//
// This type should be used for QueryLog.Metadata to ensure compatibility with
// protobuf Struct, JSON marshaling, and other serialization formats.
type Metadata map[string]interface{}

// SanitizeMetadataValue converts any Go value to a proto.Struct/JSON-compatible type.
//
// Special handling:
//   - time.Time -> string (RFC3339Nano format, zero time becomes nil)
//   - time.Duration -> string
//   - net.IP -> string (IP address as string, nil becomes nil)
//   - Pointers are dereferenced
//   - Structs are converted to map[string]interface{} via JSON
//   - Numeric types are preserved without precision loss
func SanitizeMetadataValue(v interface{}) interface{} {
	return sanitizeValue(v)
}

// SanitizeMetadata takes an existing map and returns a new Metadata map with all values sanitized.
// This is useful when you have a map built incrementally and want to sanitize it at the end.
func SanitizeMetadata(m map[string]interface{}) Metadata {
	if m == nil {
		return nil
	}

	result := make(Metadata, len(m))
	for k, v := range m {
		result[k] = sanitizeValue(v)
	}
	return result
}

// sanitizeValue recursively sanitizes a value for proto.Struct/JSON compatibility.
// It preserves numeric types and only converts special types that need handling.
func sanitizeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	// Handle special types first
	switch val := v.(type) {
	case time.Time:
		if val.IsZero() {
			return nil
		}
		return val.Format(time.RFC3339Nano)
	case *time.Time:
		if val == nil || val.IsZero() {
			return nil
		}
		return val.Format(time.RFC3339Nano)
	case time.Duration:
		return val.String()
	case net.IP:
		if val == nil {
			return nil
		}
		return val.String()
	case *net.IP:
		if val == nil || *val == nil {
			return nil
		}
		return val.String()

	// Primitives - pass through as-is (preserves int64, float64, etc.)
	case bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return val

	// Maps - recursively sanitize
	case map[string]interface{}:
		if val == nil {
			return nil
		}
		result := make(map[string]interface{}, len(val))
		for k, item := range val {
			result[k] = sanitizeValue(item)
		}
		return result

	// Slices - recursively sanitize
	case []interface{}:
		if val == nil {
			return nil
		}
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = sanitizeValue(item)
		}
		return result
	}

	// Handle typed slices and other complex types via reflection
	rv := reflect.ValueOf(v)

	// Handle pointers - dereference
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		return sanitizeValue(rv.Elem().Interface())
	}

	// Handle typed slices ([]string, []int, etc.)
	if rv.Kind() == reflect.Slice {
		if rv.IsNil() {
			return nil
		}
		result := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result[i] = sanitizeValue(rv.Index(i).Interface())
		}
		return result
	}

	// Handle typed maps
	if rv.Kind() == reflect.Map {
		if rv.IsNil() {
			return nil
		}
		result := make(map[string]interface{}, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := iter.Key()
			// Only string keys are supported
			if key.Kind() == reflect.String {
				result[key.String()] = sanitizeValue(iter.Value().Interface())
			}
		}
		return result
	}

	// For structs and other complex types, use JSON round-trip
	// This handles custom types, embedded structs, etc.
	if rv.Kind() == reflect.Struct {
		data, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		var result interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil
		}
		return result
	}

	// For anything else (channels, funcs, etc.), return nil
	return nil
}

// NewMetadata creates a new Metadata map with sanitized values from the provided key-value pairs.
// Keys must be strings, values can be any Go type and will be sanitized.
//
// Example:
//
//	metadata := NewMetadata(
//	    "user_id", 123,
//	    "timestamp", time.Now(),
//	    "ip_address", net.ParseIP("192.168.1.1"),
//	)
//
// Panics if an odd number of arguments is provided or if a key is not a string.
func NewMetadata(pairs ...interface{}) Metadata {
	if len(pairs)%2 != 0 {
		panic("NewMetadata requires an even number of arguments (key-value pairs)")
	}

	m := make(map[string]interface{}, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			panic("NewMetadata keys must be strings")
		}
		m[key] = pairs[i+1]
	}

	return SanitizeMetadata(m)
}
