package stdsql

import (
	"database/sql"
	"math/big"

	"github.com/getsynq/dwhsupport/scrapper"
)

// ValueFromRaw decodes one raw driver value, as a QueryMaps row holds it
// (read it with exec.QueryMapResult.Get), to a scrapper.Value. Read the result
// with the scrapper As* accessors.
//
// Text is kept as text: a string, []byte or sql.RawBytes is a StringValue with
// exactly the bytes the driver returned. It is never read as a UUID (16 bytes
// of text are not a binary UUID, and 36 characters are not reformatted), as
// JSON or as a Postgres array. The one exception to byte-exact is invalid UTF-8,
// which is dropped, as RunRawQuery drops it. A binary cell (BLOB, bytea, BINARY)
// arrives as []byte too and is read as text the same way, so it is lossy here
// and in RunRawQuery alike: binary data is not supported.
//
// SQL NULL is scrapper.NullValue, which no As* accessor reads. A Go type this
// does not know comes back as its fmt text, which AsString accepts, so a caller
// that must reject an unexpected driver type checks the raw cell's Go type
// before decoding it.
//
// A QueryMaps row has no column type, so this is RunRawQuery's decoding with
// the column's kind unknown: an exact numeric the driver returns as text stays
// that text, a decimal type goes through float64 (only *big.Rat, exact by its
// type, is decoded exactly), and a UUID is a UUID only when the driver hands
// back a [16]byte or a uuid.UUID. A caller that needs a value typed by its
// column uses RunRawQuery.
//
// What that means per driver, as measured on real warehouses:
//   - Snowflake returns every NUMBER, integers included, as text, so AsString
//     reads it and AsInt64 does not. A VARIANT or OBJECT arrives as its JSON
//     text, a StringValue.
//   - BigQuery returns DATE, TIME and DATETIME as civil values, decoded to text
//     ("2024-03-15"), not a TimeValue. TIMESTAMP is a TimeValue.
//   - DuckDB (and SQL Server's UNIQUEIDENTIFIER) hand a UUID back as 16 raw bytes.
//     Those are indistinguishable from 16 characters of text, so they decode to
//     meaningless text. Cast the column to text in the query.
//   - MySQL and MariaDB return text as []byte, which is kept as text.
func ValueFromRaw(v any) scrapper.Value {
	switch val := v.(type) {
	case nil:
		return scrapper.NullValue{}
	case string:
		return scrapper.StringValue(sanitizeRawString(val))
	case []byte:
		return scrapper.StringValue(sanitizeRawString(string(val)))
	case sql.RawBytes:
		return scrapper.StringValue(sanitizeRawString(string(val)))
	case *big.Rat:
		if val == nil {
			return scrapper.NullValue{}
		}
		return scrapper.DecimalValueFromRat(val)
	case *big.Int:
		// A nullable BigQuery NUMERIC or a driver big integer can arrive as a
		// typed nil pointer, which is a NULL that `v == nil` does not see.
		if val == nil {
			return scrapper.NullValue{}
		}
	}
	return convertToRawValue(v, &scrapper.QueryShapeColumn{Kind: scrapper.KindUnknown})
}
