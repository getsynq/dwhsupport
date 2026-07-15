package scrapper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// NewJsonValueFromGo converts an arbitrary driver-returned value — an
// arbitrarily nested tree of Go slices, arrays, maps, pointers and scalars — into
// a JsonValue holding canonical JSON text. It is the single normaliser used by
// every RunRawQuery implementation for complex/nested cells so the wire format
// is identical regardless of warehouse.
//
// bytesAsInts controls how a []byte / []uint8 is rendered, because Go cannot tell
// a "small-int array" from a "byte blob" — both are []uint8:
//
//   - true  → the value is a native integer array (e.g. ClickHouse Array(UInt8),
//     which the driver hands back as []uint8); render as a JSON array of numbers.
//   - false → the value is a byte blob (BYTES/BLOB column); render as a JSON
//     string (invalid UTF-8 scrubbed), matching the scalar []byte→string path.
//
// It returns ok=false only when the value cannot be represented as JSON at all;
// unknown scalar types fall back to their fmt.Stringer / fmt.Sprint form so
// nothing is silently dropped.
func NewJsonValueFromGo(v any, bytesAsInts bool) (JsonValue, bool) {
	norm, ok := jsonNormalize(v, bytesAsInts)
	if !ok {
		return "", false
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Keep raw JSON literals (json.Number for big ints/decimals) verbatim and
	// avoid escaping <, >, & which are common inside string cells.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(norm); err != nil {
		return "", false
	}
	// Encoder appends a trailing newline; trim it.
	return JsonValue(strings.TrimRight(buf.String(), "\n")), true
}

// NewJsonValueFromJSONText wraps text that is already JSON (Snowflake
// ARRAY/OBJECT/VARIANT, Redshift SUPER, Fabric/MSSQL nvarchar JSON) into a
// JsonValue, re-serialised to canonical (compact) form. Returns ok=false when
// the text is not valid JSON so the caller can fall back to StringValue.
func NewJsonValueFromJSONText(s string) (JsonValue, bool) {
	if !json.Valid([]byte(s)) {
		return "", false
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(s)); err != nil {
		return "", false
	}
	return JsonValue(buf.String()), true
}

// jsonNormalize turns v into a json.Marshal-able tree of plain Go values.
func jsonNormalize(v any, bytesAsInts bool) (any, bool) {
	switch val := v.(type) {
	case nil:
		return nil, true
	case string:
		return sanitizeJSONString(val), true
	case bool,
		int, int8, int16, int32, int64,
		uint, uint16, uint32, uint64,
		float32, float64:
		return val, true
	case uint8:
		// A bare uint8 reached here as an element of an []interface{} (e.g. a
		// ClickHouse unnamed Tuple); it is a number, not a byte blob.
		return val, true
	case json.Number:
		return val, true
	case time.Time:
		return val.UTC().Format(time.RFC3339Nano), true
	case *big.Int:
		if val == nil {
			return nil, true
		}
		return json.Number(val.String()), true
	case big.Int:
		return json.Number(val.String()), true
	case *big.Rat:
		if val == nil {
			return nil, true
		}
		return ratToJSONNumber(val), true
	case big.Rat:
		return ratToJSONNumber(&val), true
	case *big.Float:
		if val == nil {
			return nil, true
		}
		return json.Number(val.Text('f', -1)), true
	case []byte:
		if bytesAsInts {
			out := make([]any, len(val))
			for i, b := range val {
				out[i] = b
			}
			return out, true
		}
		return sanitizeJSONString(string(val)), true
	case fmt.Stringer:
		return sanitizeJSONString(val.String()), true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil, true
		}
		return jsonNormalize(rv.Elem().Interface(), bytesAsInts)
	case reflect.Slice, reflect.Array:
		// []byte was handled above; any other []uint8-kind slice arriving here is
		// a native integer array, so recurse element-wise.
		n := rv.Len()
		out := make([]any, n)
		for i := 0; i < n; i++ {
			e, ok := jsonNormalize(rv.Index(i).Interface(), bytesAsInts)
			if !ok {
				return nil, false
			}
			out[i] = e
		}
		return out, true
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			e, ok := jsonNormalize(iter.Value().Interface(), bytesAsInts)
			if !ok {
				return nil, false
			}
			out[stringifyMapKey(iter.Key())] = e
		}
		return out, true
	case reflect.Struct:
		// Types with their own JSON/text encoding (civil.Date, decimal libs, ...)
		// serialise correctly through json.Marshal as-is.
		if _, ok := v.(json.Marshaler); ok {
			return v, true
		}
		if _, ok := v.(interface{ MarshalText() ([]byte, error) }); ok {
			return v, true
		}
		return sanitizeJSONString(fmt.Sprint(v)), true
	}
	return nil, false
}

// stringifyMapKey renders a map key as a JSON object key. String keys pass
// through; everything else (ClickHouse Map(Int, ...), etc.) is stringified.
func stringifyMapKey(k reflect.Value) string {
	if k.Kind() == reflect.Interface || k.Kind() == reflect.Pointer {
		if k.IsNil() {
			return "null"
		}
		k = k.Elem()
	}
	switch k.Kind() {
	case reflect.String:
		return sanitizeJSONString(k.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(k.Uint(), 10)
	default:
		return sanitizeJSONString(fmt.Sprint(k.Interface()))
	}
}

// ratToJSONNumber renders an exact rational (e.g. a BigQuery NUMERIC/BIGNUMERIC
// surfaced as *big.Rat) as a JSON number literal. Integers stay integral;
// fractions use up to 38 decimal places (BIGNUMERIC's max scale) with trailing
// zeros trimmed, which is exact for base-10 warehouse decimals.
func ratToJSONNumber(r *big.Rat) json.Number {
	if r.IsInt() {
		return json.Number(r.Num().String())
	}
	s := r.FloatString(38)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return json.Number(s)
}

func sanitizeJSONString(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}
	return s
}
