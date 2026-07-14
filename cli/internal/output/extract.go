package output

import (
	"encoding/json"
	"fmt"

	"github.com/itchyny/gojq"
)

// ExtractField extracts a single value from a map using a jq path expression.
// Returns the string representation of the value.
func ExtractField(data any, path string) string {
	query, err := gojq.Parse(path)
	if err != nil {
		return ""
	}
	iter := query.Run(data)
	v, ok := iter.Next()
	if !ok {
		return ""
	}
	if _, isErr := v.(error); isErr {
		return ""
	}
	return formatValue(v)
}

func formatValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}
