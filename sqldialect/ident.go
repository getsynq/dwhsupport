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
// Quoting pins the case, so the constructors differ on exactly one point:
// whether an unquoted name still has to be folded the way the engine folds it.
type Ident struct {
	name string
	fold bool
}

// CanonicalIdent builds an Ident from a name already in the engine's own
// spelling — a QueryShape column, a catalog row, anything the warehouse itself
// handed back. The case is rendered as given.
//
// Text that already carries identifier quotes is unwrapped first, so a caller
// that cannot tell which form it holds still gets one layer of quotes out.
func CanonicalIdent(text string) Ident {
	name, _ := unwrapIdent(text)
	return Ident{name: name}
}

// WrittenIdent builds an Ident from text a person wrote — a `table:` entry in
// a config file, a column named in a request.
//
// Quotes the author wrote are honoured: the name inside them is the name, case
// included. An unquoted name is folded the way the engine resolves an unquoted
// reference, so that quoting it here addresses the same object it addressed
// before — `users` on Snowflake stays `"USERS"`, not a lower-case `users` the
// catalog does not hold.
func WrittenIdent(text string) Ident {
	name, quoted := unwrapIdent(text)
	return Ident{name: name, fold: !quoted}
}

// Name returns the identifier without quotes, as it will be looked up once the
// dialect's folding is applied.
func (i Ident) Name(dialect Dialect) string {
	if i.fold {
		return dialect.FoldIdent(i.name)
	}
	return i.name
}

// IsEmpty reports whether the identifier holds no name, which is how an absent
// FQN part (a database on a dialect that has none) is spelled.
func (i Ident) IsEmpty() bool {
	return i.name == ""
}

func (i Ident) ToSql(dialect Dialect) (string, error) {
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

// SplitQualifiedIdent splits a dotted identifier into its parts, on the dots
// that sit outside quotes only. `db.schema.table` is three parts; `"a.b".c` is
// two, the first of which is the single name `a.b`. Each part is returned in
// the form it was written, quotes included, for CanonicalIdent or WrittenIdent
// to interpret.
//
// A doubled closing delimiter is the escape for one literal delimiter, so it
// does not end the quote — `[a]]b.c]` is one part whose name is `a]b.c`.
//
// An unterminated quote is not an error here: the remainder is returned as one
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
			inQuote, closing = true, closingDelimiter(c)
		case c == '.':
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(c)
		}
	}
	return append(parts, current.String())
}

// unwrapIdent strips one layer of identifier quotes and undoubles any escaped
// delimiter inside, reporting whether the text carried quotes at all.
func unwrapIdent(text string) (string, bool) {
	if len(text) < 2 {
		return text, false
	}
	open, closing := text[0], text[len(text)-1]
	switch open {
	case '"', '`', '[':
	default:
		return text, false
	}
	if closing != closingDelimiter(open) {
		return text, false
	}
	inner := text[1 : len(text)-1]
	return strings.ReplaceAll(inner, string(closing)+string(closing), string(closing)), true
}

func closingDelimiter(open byte) byte {
	if open == '[' {
		return ']'
	}
	return open
}

// quoteIdent wraps name in the given delimiters, doubling any closing
// delimiter inside it — the escape every dialect here accepts.
func quoteIdent(name string, open, closing byte) string {
	escaped := strings.ReplaceAll(name, string(closing), string(closing)+string(closing))
	return string(open) + escaped + string(closing)
}

// QuoteIdentWithDoubleQuotes quotes with `"…"`, unconditionally.
func QuoteIdentWithDoubleQuotes(name string) string { return quoteIdent(name, '"', '"') }

// QuoteIdentWithBackticks quotes with “ `…` “, unconditionally.
func QuoteIdentWithBackticks(name string) string { return quoteIdent(name, '`', '`') }

// QuoteIdentWithBrackets quotes with `[…]`, unconditionally.
func QuoteIdentWithBrackets(name string) string { return quoteIdent(name, '[', ']') }
