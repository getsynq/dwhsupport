package querylogs

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStringValue(t *testing.T) {
	v := StringValue("hello")
	require.NotNil(t, v)
	assert.Equal(t, "hello", v.GetStringValue())
}

func TestStringPtrValue(t *testing.T) {
	// Non-nil pointer
	s := "hello"
	v := StringPtrValue(&s)
	require.NotNil(t, v)
	assert.Equal(t, "hello", v.GetStringValue())

	// Nil pointer
	assert.Nil(t, StringPtrValue(nil))
}

func TestTrimmedStringPtrValue(t *testing.T) {
	// Non-nil pointer with whitespace
	s := "  hello  "
	v := TrimmedStringPtrValue(&s)
	require.NotNil(t, v)
	assert.Equal(t, "hello", v.GetStringValue())

	// Empty after trim
	s2 := "   "
	assert.Nil(t, TrimmedStringPtrValue(&s2))

	// Nil pointer
	assert.Nil(t, TrimmedStringPtrValue(nil))
}

func TestIntValue(t *testing.T) {
	v := IntValue(42)
	require.NotNil(t, v)
	assert.Equal(t, float64(42), v.GetNumberValue())
}

func TestIntPtrValue(t *testing.T) {
	// Non-nil pointer
	i := int64(42)
	v := IntPtrValue(&i)
	require.NotNil(t, v)
	assert.Equal(t, float64(42), v.GetNumberValue())

	// Nil pointer
	assert.Nil(t, IntPtrValue(nil))
}

func TestFloatValue(t *testing.T) {
	v := FloatValue(3.14)
	require.NotNil(t, v)
	assert.Equal(t, 3.14, v.GetNumberValue())
}

func TestBoolValue(t *testing.T) {
	v := BoolValue(true)
	require.NotNil(t, v)
	assert.True(t, v.GetBoolValue())
}

func TestBoolPtrValue(t *testing.T) {
	// Non-nil pointer
	b := true
	v := BoolPtrValue(&b)
	require.NotNil(t, v)
	assert.True(t, v.GetBoolValue())

	// Nil pointer
	assert.Nil(t, BoolPtrValue(nil))
}

func TestTimeValue(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	v := TimeValue(ts)
	require.NotNil(t, v)
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", v.GetStringValue())

	// Zero time
	var zeroTime time.Time
	assert.Nil(t, TimeValue(zeroTime))
}

func TestTimePtrValue(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	v := TimePtrValue(&ts)
	require.NotNil(t, v)
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", v.GetStringValue())

	// Nil pointer
	assert.Nil(t, TimePtrValue(nil))

	// Zero time
	var zeroTime time.Time
	assert.Nil(t, TimePtrValue(&zeroTime))
}

func TestIPValue(t *testing.T) {
	ip := net.ParseIP("192.168.1.1")
	v := IPValue(ip)
	require.NotNil(t, v)
	assert.Equal(t, "192.168.1.1", v.GetStringValue())

	// Nil IP
	var nilIP net.IP
	assert.Nil(t, IPValue(nilIP))
}

func TestIPPtrValue(t *testing.T) {
	ip := net.ParseIP("192.168.1.1")
	v := IPPtrValue(&ip)
	require.NotNil(t, v)
	assert.Equal(t, "192.168.1.1", v.GetStringValue())

	// Nil pointer
	assert.Nil(t, IPPtrValue(nil))
}

func TestStringListValue(t *testing.T) {
	v := StringListValue([]string{"a", "b", "c"})
	require.NotNil(t, v)
	list := v.GetListValue()
	require.NotNil(t, list)
	assert.Len(t, list.GetValues(), 3)
	assert.Equal(t, "a", list.GetValues()[0].GetStringValue())

	// Empty slice
	assert.Nil(t, StringListValue([]string{}))
	assert.Nil(t, StringListValue(nil))
}

func TestStructValue(t *testing.T) {
	fields := map[string]*structpb.Value{
		"key": StringValue("value"),
	}
	v := StructValue(fields)
	require.NotNil(t, v)
	s := v.GetStructValue()
	require.NotNil(t, s)
	assert.Equal(t, "value", s.GetFields()["key"].GetStringValue())

	// Empty map
	assert.Nil(t, StructValue(map[string]*structpb.Value{}))
}

func TestNewMetadataStruct(t *testing.T) {
	fields := map[string]*structpb.Value{
		"string": StringValue("value"),
		"int":    IntValue(42),
		"bool":   BoolValue(true),
		"nil":    nil, // Should be filtered out
	}

	result := NewMetadataStruct(fields)
	require.NotNil(t, result)

	// Check fields
	f := result.GetFields()
	assert.Equal(t, "value", f["string"].GetStringValue())
	assert.Equal(t, float64(42), f["int"].GetNumberValue())
	assert.True(t, f["bool"].GetBoolValue())
	assert.NotContains(t, f, "nil") // nil values should be filtered

	// Nil map
	assert.Nil(t, NewMetadataStruct(nil))

	// Map with only nil values
	assert.Nil(t, NewMetadataStruct(map[string]*structpb.Value{"a": nil, "b": nil}))
}
