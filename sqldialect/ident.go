package sqldialect

import (
	"strings"
	"unicode"
)

// Ident is one SQL identifier — a database, schema, table, column or alias
// name — carried as a name rather than as SQL text, and always rendered
// wrapped in the dialect's quote characters.
//
// The type exists because a bare string does not say which of the two it is,
// and guessing is wrong in both directions:
//
//   - read as a name that needs no quoting, a reserved word (`order`, `group`)
//     reaches the engine bare and the statement fails to parse;
//   - read as a name, text whose author already quoted it (`"events"`) is
//     quoted a second time, and `"""events"""` addresses a different object;
//   - and splitting `"public.table"` on its dot turns one identifier into two.
//
// Every "quote only when needed" helper in this package decides by character
// class, which cannot see a reserved word, so an Ident quotes unconditionally.
//
// Nothing is decoded at construction, because both the delimiters and the
// escape inside them belong to the dialect: BigQuery writes a backtick inside
// a quoted identifier as a backslash followed by the backtick, after the
// manner of a string literal, and everyone else doubles the delimiter.
type Ident struct {
	text      string
	written   bool
	canonical bool
}

// CanonicalIdent builds an Ident from a name already in the engine's own
// spelling — a QueryShape column, a catalog row, anything the warehouse itself
// handed back. It is rendered exactly as given, case and all.
//
// Nothing is unwrapped, because at this end of the pipe a delimiter is part of
// the name: a Postgres column really called `"foo"`, quotes included, comes
// back from QueryShape spelled that way, and reading the quotes as syntax
// would address the column `foo` instead. Text a person wrote is the other
// case — see WrittenIdent.
func CanonicalIdent(name string) Ident {
	return Ident{text: name, canonical: true}
}

// WrittenIdent builds an Ident from text a person wrote — a `table:` entry in
// a config file, a column named in a request.
//
// Quotes the author wrote are honoured: the name inside them is the name, case
// included. An unquoted name is folded the way the engine resolves an unquoted
// reference, so that quoting it here addresses the same object it addressed
// before — `users` against Snowflake stays `"USERS"`, not a lower-case `users`
// the catalog does not hold. Dialect.FoldIdent says what that does and, just
// as importantly, does not touch.
func WrittenIdent(text string) Ident {
	return Ident{text: text, written: true}
}

// Name returns the identifier without quotes, under the case it resolves to.
func (i Ident) Name(dialect Dialect) string {
	if i.canonical {
		return i.text
	}
	name, quoted := dialect.UnquoteIdent(i.text)
	if i.written && !quoted {
		return dialect.FoldIdent(name)
	}
	return name
}

// IsEmpty reports whether the identifier holds no text, which is how an absent
// FQN part — a database on a dialect that has none — is spelled.
func (i Ident) IsEmpty() bool {
	return i.text == ""
}

func (i Ident) ToSql(dialect Dialect) (string, error) {
	// An empty identifier is not a thing on any engine here — BigQuery says so
	// outright, "Can't be empty" — and rendering an empty delimiter pair would
	// turn an absent FQN part into a syntax error. QualifiedIdent skips them,
	// so this only catches a bare empty Ident rendered on its own.
	if i.IsEmpty() {
		return "", nil
	}
	return dialect.QuoteIdent(i.Name(dialect)), nil
}

func (i Ident) IsTextExpr() {}

var (
	_ Expr     = Ident{}
	_ TextExpr = Ident{}
)

// QualifiedIdentExpr is a dot-joined run of identifiers, each quoted on its
// own. The parts are never joined first and quoted after: `"public.table"` is
// one object and `"public"."table"` is another.
type QualifiedIdentExpr struct {
	parts []Ident
}

var (
	_ Expr      = (*QualifiedIdentExpr)(nil)
	_ TableExpr = (*QualifiedIdentExpr)(nil)
)

// QualifiedIdent joins the parts with dots, skipping empty ones so a caller can
// pass an absent database or schema straight through.
func QualifiedIdent(parts ...Ident) *QualifiedIdentExpr {
	return &QualifiedIdentExpr{parts: parts}
}

func (q *QualifiedIdentExpr) ToSql(dialect Dialect) (string, error) {
	rendered := make([]string, 0, len(q.parts))
	for _, part := range q.parts {
		if part.IsEmpty() {
			continue
		}
		sql, err := part.ToSql(dialect)
		if err != nil {
			return "", err
		}
		rendered = append(rendered, sql)
	}
	return strings.Join(rendered, "."), nil
}

func (q *QualifiedIdentExpr) IsTableExpr() {}

// identQuoting is one dialect's identifier delimiters plus the escape it uses
// for the closing one. Each dialect names its own in dialect_<name>.go.
type identQuoting struct {
	open, closing byte
	// backslash marks a dialect that escapes the delimiter the way a string
	// literal does, `\<delim>`, rather than by doubling it. BigQuery is the
	// only one: "Quoted identifiers have the same escape sequences as string
	// literals."
	backslash bool
}

var (
	identQuotingDoubleQuotes    = identQuoting{open: '"', closing: '"'}
	identQuotingBackticks       = identQuoting{open: '`', closing: '`'}
	identQuotingBackticksEscape = identQuoting{open: '`', closing: '`', backslash: true}
	identQuotingBrackets        = identQuoting{open: '[', closing: ']'}
)

// quote wraps name in the delimiters, escaping the closing delimiter inside it.
func (q identQuoting) quote(name string) string {
	var escaped string
	if q.backslash {
		escaped = strings.ReplaceAll(name, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, string(q.closing), `\`+string(q.closing))
	} else {
		escaped = strings.ReplaceAll(name, string(q.closing), string(q.closing)+string(q.closing))
	}
	return string(q.open) + escaped + string(q.closing)
}

// unquote strips one layer of identifier quotes and decodes the escape inside,
// reporting whether the text carried quotes at all.
//
// A delimiter pair this dialect does not itself use is recognised and stripped
// too: someone writing `"events"` for BigQuery has said plainly which part is
// the name, and reading it as an unquoted name would quote the quotes into it.
func (q identQuoting) unquote(text string) (string, bool) {
	if len(text) < 2 {
		return text, false
	}
	open, closing := text[0], text[len(text)-1]
	if identClosingDelimiter(open) == 0 || closing != identClosingDelimiter(open) {
		return text, false
	}
	inner := text[1 : len(text)-1]
	if open == q.open && q.backslash {
		return unescapeBackslash(inner), true
	}
	return strings.ReplaceAll(inner, string(closing)+string(closing), string(closing)), true
}

// unescapeBackslash decodes the escape sequences a quoted identifier accepts
// on a dialect that escapes like a string literal — BigQuery, whose reference
// says so outright. Skipping the byte after a backslash is not enough:
// `\x41` is the identifier `A`, and dropping only the backslash would leave
// `x41`, a different object.
//
// A sequence the reference does not define is an error on the engine, so the
// backslash is simply dropped there and the engine gets to complain about the
// name rather than about our reading of it.
func unescapeBackslash(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var out strings.Builder
	out.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			out.WriteByte(s[i])
			continue
		}
		i++
		switch c := s[i]; c {
		case 'a':
			out.WriteByte('\a')
		case 'b':
			out.WriteByte('\b')
		case 'f':
			out.WriteByte('\f')
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 't':
			out.WriteByte('\t')
		case 'v':
			out.WriteByte('\v')
		case 'x', 'X':
			if r, n := readIdentEscapeHex(s[i+1:], 2); n > 0 {
				out.WriteRune(r)
				i += n
				continue
			}
			out.WriteByte(c)
		case 'u':
			if r, n := readIdentEscapeHex(s[i+1:], 4); n > 0 {
				out.WriteRune(r)
				i += n
				continue
			}
			out.WriteByte(c)
		case 'U':
			if r, n := readIdentEscapeHex(s[i+1:], 8); n > 0 {
				out.WriteRune(r)
				i += n
				continue
			}
			out.WriteByte(c)
		default:
			if r, n := readIdentEscapeOctal(s[i:]); n > 0 {
				out.WriteRune(r)
				i += n - 1
				continue
			}
			out.WriteByte(c)
		}
	}
	return out.String()
}

// readIdentEscapeHex reads exactly n hex digits, returning the rune they spell
// and how many bytes it consumed. It returns 0 when the digits are not there,
// which leaves the caller emitting the escape character literally.
func readIdentEscapeHex(s string, n int) (rune, int) {
	if len(s) < n {
		return 0, 0
	}
	var value rune
	for i := 0; i < n; i++ {
		digit := hexDigitValue(s[i])
		if digit < 0 {
			return 0, 0
		}
		value = value<<4 | rune(digit)
	}
	return value, n
}

// readIdentEscapeOctal reads the three-octal-digit form, `\ooo`.
func readIdentEscapeOctal(s string) (rune, int) {
	if len(s) < 3 {
		return 0, 0
	}
	var value rune
	for i := 0; i < 3; i++ {
		if s[i] < '0' || s[i] > '7' {
			return 0, 0
		}
		value = value<<3 | rune(s[i]-'0')
	}
	return value, 3
}

func hexDigitValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return -1
	}
}

func identClosingDelimiter(open byte) byte {
	switch open {
	case '[':
		return ']'
	case '"', '`':
		return open
	default:
		return 0
	}
}

// identFolding describes how one dialect resolves an unquoted reference: which
// way the case goes, and which names it would have accepted unquoted at all.
//
// The second half is what keeps the fold honest. Folding a name the engine
// would have rejected unquoted preserves nothing — there was no unquoted
// resolution — and only invents a different object: `Created At` would become
// `"CREATED AT"`. Folding a name the engine *would* have taken is mandatory
// for the same reason in reverse: Oracle resolves `sales#q1` as `SALES#Q1`, so
// quoting it without folding addresses something else.
type identFolding struct {
	upper bool
	// extra lists the non-alphanumeric ASCII characters this dialect allows
	// after the first character of an unquoted identifier, beyond `_`.
	extra string
	// unicode marks a dialect whose unquoted identifiers may carry letters
	// from outside ASCII — Postgres takes letters with diacritics and
	// non-Latin letters, Oracle the database character set.
	unicode bool
}

var (
	identFoldingLower       = identFolding{extra: "$", unicode: true}
	identFoldingUpper       = identFolding{upper: true, extra: "$"}
	identFoldingUpperOracle = identFolding{upper: true, extra: "$#", unicode: true}
	// Db2 takes #, $ and @ inside an unquoted name, and letters outside ASCII.
	identFoldingUpperDb2 = identFolding{upper: true, extra: "$#@", unicode: true}
)

// fold returns the name an unquoted reference resolves to, or the name
// untouched when it could not have been written unquoted here.
func (f identFolding) fold(name string) string {
	if !f.canBeUnquoted(name) {
		return name
	}
	if f.upper {
		return strings.ToUpper(name)
	}
	return strings.ToLower(name)
}

func (f identFolding) canBeUnquoted(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case i > 0 && r >= '0' && r <= '9':
		case i > 0 && strings.ContainsRune(f.extra, r):
		case f.unicode && r > unicode.MaxASCII && unicode.IsLetter(r):
		default:
			return false
		}
	}
	return true
}

// SplitQualifiedIdent splits a dotted identifier into its parts, on the dots
// that sit outside quotes only. `db.schema.table` is three parts; `"a.b".c` is
// two, the first of which is the single name `a.b`. Each part is returned in
// the form it was written, quotes included, for CanonicalIdent or WrittenIdent
// to interpret.
//
// It takes no dialect because it runs where none is known yet — parsing a
// `table:` entry — so it knows every dialect's delimiters and has to decide,
// without knowing which one applies, where each quote ends. A doubled closing
// delimiter never ends a quote. A backslash escapes the character after it
// only inside backticks, which is where BigQuery's escaped delimiter lives;
// inside a double quote or a bracket a backslash is a literal character on
// every dialect that uses those, so Postgres `"a\\".orders` still splits into
// two parts. The one shape left ambiguous is a MySQL backtick name ending in a
// backslash, which reads as escaping its closing backtick and so comes back as
// one part; the benefit is that BigQuery's escaped backtick does not cut a
// name in half.
//
// An unterminated quote is not an error here: the remainder comes back as one
// part, and whichever engine receives it reports the problem in its own terms.
func SplitQualifiedIdent(text string) []string {
	var (
		parts   []string
		current strings.Builder
		closing byte
		inQuote bool
	)
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case inQuote && closing == '`' && c == '\\' && i+1 < len(text):
			current.WriteByte(c)
			i++
			current.WriteByte(text[i])
		case inQuote && c == closing:
			current.WriteByte(c)
			if i+1 < len(text) && text[i+1] == closing {
				current.WriteByte(closing)
				i++
				continue
			}
			inQuote = false
		case inQuote:
			current.WriteByte(c)
		case c == '"' || c == '`' || c == '[':
			current.WriteByte(c)
			inQuote, closing = true, identClosingDelimiter(c)
		case c == '.':
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(c)
		}
	}
	return append(parts, current.String())
}
