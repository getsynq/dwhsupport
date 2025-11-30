package querylogs

import (
	"net"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// Helper functions to convert Go types to structpb.Value for efficient metadata construction.
// These avoid reflection and validation overhead by creating Values directly.

// StringValue creates a structpb.Value from a string.
func StringValue(v string) *structpb.Value {
	return structpb.NewStringValue(v)
}

// StringPtrValue creates a structpb.Value from a string pointer. Returns nil if pointer is nil.
func StringPtrValue(v *string) *structpb.Value {
	if v == nil {
		return nil
	}
	return structpb.NewStringValue(*v)
}

// TrimmedStringPtrValue creates a structpb.Value from a string pointer, trimming whitespace.
// Returns nil if pointer is nil or if the trimmed string is empty.
func TrimmedStringPtrValue(v *string) *structpb.Value {
	if v == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*v)
	if trimmed == "" {
		return nil
	}
	return structpb.NewStringValue(trimmed)
}

// IntValue creates a structpb.Value from an int64.
func IntValue(v int64) *structpb.Value {
	return structpb.NewNumberValue(float64(v))
}

// IntPtrValue creates a structpb.Value from an int64 pointer. Returns nil if pointer is nil.
func IntPtrValue(v *int64) *structpb.Value {
	if v == nil {
		return nil
	}
	return structpb.NewNumberValue(float64(*v))
}

// Int32PtrValue creates a structpb.Value from an int32 pointer. Returns nil if pointer is nil.
func Int32PtrValue(v *int32) *structpb.Value {
	if v == nil {
		return nil
	}
	return structpb.NewNumberValue(float64(*v))
}

// UintValue creates a structpb.Value from a uint64.
func UintValue(v uint64) *structpb.Value {
	return structpb.NewNumberValue(float64(v))
}

// Uint32Value creates a structpb.Value from a uint32.
func Uint32Value(v uint32) *structpb.Value {
	return structpb.NewNumberValue(float64(v))
}

// Uint8Value creates a structpb.Value from a uint8.
func Uint8Value(v uint8) *structpb.Value {
	return structpb.NewNumberValue(float64(v))
}

// Int32Value creates a structpb.Value from an int32.
func Int32Value(v int32) *structpb.Value {
	return structpb.NewNumberValue(float64(v))
}

// FloatValue creates a structpb.Value from a float64.
func FloatValue(v float64) *structpb.Value {
	return structpb.NewNumberValue(v)
}

// FloatPtrValue creates a structpb.Value from a float64 pointer. Returns nil if pointer is nil.
func FloatPtrValue(v *float64) *structpb.Value {
	if v == nil {
		return nil
	}
	return structpb.NewNumberValue(*v)
}

// BoolValue creates a structpb.Value from a bool.
func BoolValue(v bool) *structpb.Value {
	return structpb.NewBoolValue(v)
}

// BoolPtrValue creates a structpb.Value from a bool pointer. Returns nil if pointer is nil.
func BoolPtrValue(v *bool) *structpb.Value {
	if v == nil {
		return nil
	}
	return structpb.NewBoolValue(*v)
}

// TimeValue creates a structpb.Value from a time.Time as RFC3339Nano string.
// Returns nil for zero time.
func TimeValue(v time.Time) *structpb.Value {
	if v.IsZero() {
		return nil
	}
	return structpb.NewStringValue(v.Format(time.RFC3339Nano))
}

// TimePtrValue creates a structpb.Value from a time.Time pointer. Returns nil if pointer is nil or zero.
func TimePtrValue(v *time.Time) *structpb.Value {
	if v == nil || v.IsZero() {
		return nil
	}
	return structpb.NewStringValue(v.Format(time.RFC3339Nano))
}

// DurationValue creates a structpb.Value from a time.Duration as string.
func DurationValue(v time.Duration) *structpb.Value {
	return structpb.NewStringValue(v.String())
}

// IPValue creates a structpb.Value from a net.IP as string. Returns nil if IP is nil.
func IPValue(v net.IP) *structpb.Value {
	if v == nil {
		return nil
	}
	return structpb.NewStringValue(v.String())
}

// IPPtrValue creates a structpb.Value from a net.IP pointer. Returns nil if pointer is nil.
func IPPtrValue(v *net.IP) *structpb.Value {
	if v == nil || *v == nil {
		return nil
	}
	return structpb.NewStringValue(v.String())
}

// UInt16Value creates a structpb.Value from a uint16.
func UInt16Value(v uint16) *structpb.Value {
	return structpb.NewNumberValue(float64(v))
}

// StructValue creates a structpb.Value from a map of Values.
func StructValue(fields map[string]*structpb.Value) *structpb.Value {
	if len(fields) == 0 {
		return nil
	}
	return structpb.NewStructValue(&structpb.Struct{Fields: fields})
}

// StringListValue creates a structpb.Value from a string slice.
func StringListValue(v []string) *structpb.Value {
	if len(v) == 0 {
		return nil
	}
	values := make([]*structpb.Value, len(v))
	for i, s := range v {
		values[i] = structpb.NewStringValue(s)
	}
	return structpb.NewListValue(&structpb.ListValue{Values: values})
}

// NewMetadataStruct creates a structpb.Struct from a map of Values, filtering out nil values.
// Returns nil if the resulting map is empty.
func NewMetadataStruct(fields map[string]*structpb.Value) *structpb.Struct {
	if len(fields) == 0 {
		return nil
	}
	// Filter out nil values
	filtered := make(map[string]*structpb.Value, len(fields))
	for k, v := range fields {
		if v != nil {
			filtered[k] = v
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &structpb.Struct{Fields: filtered}
}
