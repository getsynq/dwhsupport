package stdsql

import (
	"encoding/json"
	"strconv"
	"strings"
)

// parsePostgresArrayLiteral parses a PostgreSQL array output literal — the text
// form lib/pq (and the Redshift driver) hand back for array columns when
// scanning into an untyped destination, e.g. `{1,2,3}`, `{a,b}`, `{"a,b","c"}`,
// `{{1,2},{3,4}}`, `{NULL,1}`, `{}`. It returns a JSON-able tree ([]any of
// json.Number / string / bool / nil / nested []any) and ok=false when the input
// is not a well-formed array literal (so the caller keeps it as a string).
//
// elemType is the element's Postgres type name without the leading underscore
// (e.g. "INT4", "TEXT", "BOOL"); it decides how unquoted tokens are typed.
// Quoted tokens are always strings; the bareword NULL (case-insensitive) is the
// SQL null, while a quoted "NULL" is the literal string.
func parsePostgresArrayLiteral(s, elemType string) (any, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") {
		return nil, false
	}
	p := &pgArrayParser{s: s, elem: strings.ToUpper(elemType)}
	v, ok := p.parseArray()
	if !ok {
		return nil, false
	}
	p.skipSpace()
	if p.i != len(p.s) {
		return nil, false
	}
	return v, true
}

type pgArrayParser struct {
	s    string
	i    int
	elem string
}

func (p *pgArrayParser) skipSpace() {
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

// parseArray parses a `{...}` at the current position.
func (p *pgArrayParser) parseArray() ([]any, bool) {
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != '{' {
		return nil, false
	}
	p.i++ // consume '{'
	out := []any{}
	p.skipSpace()
	if p.i < len(p.s) && p.s[p.i] == '}' {
		p.i++ // empty array
		return out, true
	}
	for {
		v, ok := p.parseElement()
		if !ok {
			return nil, false
		}
		out = append(out, v)
		p.skipSpace()
		if p.i >= len(p.s) {
			return nil, false
		}
		switch p.s[p.i] {
		case ',':
			p.i++
			continue
		case '}':
			p.i++
			return out, true
		default:
			return nil, false
		}
	}
}

// parseElement parses a single element: a nested array, a quoted string, or a
// bareword.
func (p *pgArrayParser) parseElement() (any, bool) {
	p.skipSpace()
	if p.i >= len(p.s) {
		return nil, false
	}
	switch p.s[p.i] {
	case '{':
		return p.parseArray()
	case '"':
		return p.parseQuoted()
	default:
		return p.parseBareword()
	}
}

// parseQuoted parses a `"..."` token with PG escaping (\" and \\).
func (p *pgArrayParser) parseQuoted() (any, bool) {
	p.i++ // consume opening quote
	var b strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch c {
		case '\\':
			if p.i+1 >= len(p.s) {
				return nil, false
			}
			b.WriteByte(p.s[p.i+1])
			p.i += 2
		case '"':
			p.i++ // consume closing quote
			return b.String(), true
		default:
			b.WriteByte(c)
			p.i++
		}
	}
	return nil, false // unterminated
}

// parseBareword parses an unquoted token up to the next delimiter and types it
// according to the element type. A bareword NULL is the SQL null.
func (p *pgArrayParser) parseBareword() (any, bool) {
	start := p.i
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ',', '}', '{':
			goto done
		default:
			p.i++
		}
	}
done:
	tok := strings.TrimSpace(p.s[start:p.i])
	if tok == "" {
		return nil, false
	}
	if strings.EqualFold(tok, "NULL") {
		return nil, true
	}
	return p.typeToken(tok), true
}

// typeToken converts an unquoted, non-null token to the JSON value implied by
// the array's element type. Unknown/unparseable tokens fall back to a string so
// the output is never wrong, only less specific.
func (p *pgArrayParser) typeToken(tok string) any {
	switch p.elem {
	case "INT2", "INT4", "INT8", "OID":
		if _, err := strconv.ParseInt(tok, 10, 64); err == nil {
			return json.Number(tok)
		}
	case "FLOAT4", "FLOAT8", "NUMERIC":
		if _, err := strconv.ParseFloat(tok, 64); err == nil {
			return json.Number(tok)
		}
	case "BOOL":
		switch tok {
		case "t":
			return true
		case "f":
			return false
		}
	}
	return tok
}
