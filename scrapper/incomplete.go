package scrapper

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// OmittedPart names what a query left out of an object it could not fully read.
type OmittedPart string

const (
	// OmittedObjects — the objects in the omission's scope are missing from the
	// result altogether. A caller treating the result as a complete listing must
	// not read their absence as their removal.
	OmittedObjects OmittedPart = "objects"
	// OmittedColumns — the object is listed, but its columns could not be read,
	// so it comes back with none.
	OmittedColumns OmittedPart = "columns"
	// OmittedDescriptions — the object is listed without its description.
	OmittedDescriptions OmittedPart = "descriptions"
	// OmittedDefinitions — the objects' SQL definitions are missing.
	OmittedDefinitions OmittedPart = "definitions"
	// OmittedMetrics — the objects' metrics are missing.
	OmittedMetrics OmittedPart = "metrics"
)

// Omission is one part of the warehouse a query did not read. An empty Schema
// covers the whole database, an empty Table the whole schema.
type Omission struct {
	Database string
	Schema   string
	Table    string
	Part     OmittedPart
	Err      error
}

func (o Omission) Scope() string {
	return strings.Join(nonEmpty(o.Database, o.Schema, o.Table), ".")
}

func (o Omission) String() string {
	return fmt.Sprintf("%s of %s: %v", o.Part, o.Scope(), o.Err)
}

type incompleteResultsKey struct{}

// WithIncompleteResults tells the scrappers that support it that the caller
// handles an *IncompleteError, so a query the warehouse refuses in part returns
// what it read instead of failing. A caller that does not ask gets the
// behaviour it always had.
func WithIncompleteResults(ctx context.Context) context.Context {
	return context.WithValue(ctx, incompleteResultsKey{}, true)
}

// IncompleteResultsAccepted reports whether the caller asked for incomplete
// results with WithIncompleteResults.
func IncompleteResultsAccepted(ctx context.Context) bool {
	accepted, _ := ctx.Value(incompleteResultsKey{}).(bool)
	return accepted
}

// IncompleteError is returned beside the rows a query did read when the
// warehouse refused part of what it asked for. The rows are valid; they are
// missing exactly what the omissions describe.
//
// Only a caller that asked with WithIncompleteResults receives one. It should
// publish the rows and report the omissions, and must not treat an object an
// OmittedObjects omission covers as deleted.
type IncompleteError struct {
	Omissions []Omission
}

func (e *IncompleteError) Error() string {
	if len(e.Omissions) == 1 {
		return "incomplete result, left out " + e.Omissions[0].String()
	}
	parts := make([]string, len(e.Omissions))
	for i, o := range e.Omissions {
		parts[i] = o.String()
	}
	return fmt.Sprintf("incomplete result, left out %d parts: %s", len(e.Omissions), strings.Join(parts, "; "))
}

// Incomplete returns an *IncompleteError for the omissions, or nil when there
// are none, so a query can end with `return rows, scrapper.Incomplete(omissions)`.
func Incomplete(omissions []Omission) error {
	if len(omissions) == 0 {
		return nil
	}
	return &IncompleteError{Omissions: omissions}
}

// AsIncomplete reports whether err is an *IncompleteError, and returns it.
func AsIncomplete(err error) (*IncompleteError, bool) {
	var incomplete *IncompleteError
	if errors.As(err, &incomplete) {
		return incomplete, true
	}
	return nil, false
}

func nonEmpty(values ...string) []string {
	out := values[:0:0]
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
