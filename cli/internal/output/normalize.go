package output

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var protoMarshaler = protojson.MarshalOptions{
	EmitUnpopulated: false,
	Multiline:       false,
}

// Normalize converts a value to map[string]any via JSON round-trip.
// Supports proto.Message and arbitrary Go structs/maps.
func Normalize(v any) (any, error) {
	var jsonBytes []byte
	var err error

	switch msg := v.(type) {
	case proto.Message:
		jsonBytes, err = protoMarshaler.Marshal(msg)
	default:
		jsonBytes, err = json.Marshal(v)
	}
	if err != nil {
		return nil, err
	}

	var result any
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// NormalizeMap converts a value to map[string]any. Use for single objects.
func NormalizeMap(v any) (map[string]any, error) {
	raw, err := Normalize(v)
	if err != nil {
		return nil, err
	}
	if m, ok := raw.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"value": raw}, nil
}

// NormalizeList converts a slice of values to []any (each element normalized).
func NormalizeList[T any](items []T) ([]any, error) {
	result := make([]any, 0, len(items))
	for _, item := range items {
		m, err := Normalize(item)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, nil
}
