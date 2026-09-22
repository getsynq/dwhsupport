package sqldialect

import "strings"

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
// a quoted identifier as `\“ after the manner of a string literal, and
// everyone else doubles the delimiter.
type Ident struct {
	text    string
	written bool
}

// CanonicalIdent builds an Ident from a name already in the engine's own
// spelling — a QueryShape column, a catalog row, anything the warehouse itself
// handed back. The case is rendered as given.
//
// Text that already carries identifier quotes is unwrapped first, so a caller
// that cannot tell which form it holds still gets one layer of quotes out.
func CanonicalIdent(text string) Ident {
	return Ident{text: text}
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

func unescapeBackslash(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var out strings.Builder
	out.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		out.WriteByte(s[i])
	}
	return out.String()
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

// foldIdentASCII folds the ASCII letters of a name the way an engine folds an
// unquoted reference — but only when the engine would have taken the name
// unquoted in the first place.
//
// A name carrying a space, a dash, a dot or a non-ASCII letter was never a
// valid unquoted reference, so there is no unquoted resolution to preserve and
// folding it would only invent a different object: `Created At` stays
// `"Created At"` rather than becoming `"CREATED AT"`. The folding is ASCII-only
// for the same reason — every dialect here restricts an unquoted identifier to
// ASCII letters, digits and underscore, so a Unicode letter can only have
// arrived quoted.
func foldIdentASCII(name string, upper bool) string {
	if !isUnquotedIdent(name) {
		return name
	}
	out := []byte(name)
	for i, c := range out {
		switch {
		case upper && c >= 'a' && c <= 'z':
			out[i] = c - ('a' - 'A')
		case !upper && c >= 'A' && c <= 'Z':
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out)
}

// isUnquotedIdent reports whether a name could have been written without
// quotes: an ASCII letter, underscore or dollar to start, then letters, digits,
// underscores and dollars. It is the intersection of the dialects' unquoted
// rules rather than any single one, which is the safe side — a name it rejects
// is quoted as written, and a quoted name always resolves.
func isUnquotedIdent(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		case c >= '0' && c <= '9' && i > 0:
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
// config file, long before a connection is opened — and it does not need one
// to find where a quote ends. Both escapes are honoured, a doubled closing
// delimiter and a backslashed one. The cost is a name ending in a literal
// backslash on a doubling dialect (MySQL “ `a\` “.`b`), which reads here as
// one part; the benefit is that BigQuery's `\“ does not cut a name in half.
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
		case inQuote && c == '\\' && i+1 < len(text):
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
