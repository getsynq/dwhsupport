package scrapper_test

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/assert"
)

type stubLeaf struct{}

func (stubLeaf) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }
func (stubLeaf) DialectType() string                 { return "stub" }
func (stubLeaf) SqlDialect() sqldialect.Dialect      { return nil }
func (stubLeaf) IsPermissionError(error) bool        { return false }
func (stubLeaf) Close() error                        { return nil }
func (stubLeaf) ValidateConfiguration(context.Context) ([]string, error) {
	return nil, nil
}
func (stubLeaf) QueryCatalog(context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return nil, nil
}
func (stubLeaf) QueryTableMetrics(context.Context, time.Time) ([]*scrapper.TableMetricsRow, error) {
	return nil, nil
}
func (stubLeaf) QuerySqlDefinitions(context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return nil, nil
}
func (stubLeaf) QueryTables(context.Context, ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	return nil, nil
}
func (stubLeaf) QueryDatabases(context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, nil
}
func (stubLeaf) QuerySegments(context.Context, string, ...any) ([]*scrapper.SegmentRow, error) {
	return nil, nil
}
func (stubLeaf) QueryCustomMetrics(context.Context, string, ...any) ([]*scrapper.CustomMetricsRow, error) {
	return nil, nil
}
func (stubLeaf) QueryShape(context.Context, string) ([]*scrapper.QueryShapeColumn, error) {
	return nil, nil
}
func (stubLeaf) RunRawQuery(context.Context, string) (scrapper.RawQueryRowIterator, error) {
	return nil, nil
}
func (stubLeaf) QueryTableConstraints(context.Context) ([]*scrapper.TableConstraintRow, error) {
	return nil, nil
}

// extrasLeaf embeds stubLeaf so it satisfies scrapper.Scrapper, and also
// exposes an extra method that callers may want to reach past decorators.
type extrasLeaf struct {
	stubLeaf
	called bool
}

type extraCapable interface {
	ExtraCall() string
}

func (e *extrasLeaf) ExtraCall() string {
	e.called = true
	return "ok"
}

func TestAs_ReachesPastDecorators(t *testing.T) {
	leaf := &extrasLeaf{}
	wrapped := sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(leaf))

	// Directly asserting on the outermost wrapper fails — it does not itself
	// implement extraCapable.
	_, ok := any(wrapped).(extraCapable)
	assert.False(t, ok, "sanity: direct assertion on wrapped must fail")

	found, ok := scrapper.As[extraCapable](wrapped)
	if assert.True(t, ok, "As must reach extrasLeaf through two decorators") {
		assert.Equal(t, "ok", found.ExtraCall())
		assert.True(t, leaf.called)
	}
}

func TestAs_ReachesLeafConcreteType(t *testing.T) {
	leaf := &extrasLeaf{}
	wrapped := sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(leaf))

	found, ok := scrapper.As[*extrasLeaf](wrapped)
	if assert.True(t, ok) {
		assert.Same(t, leaf, found)
	}
}

func TestAs_ReturnsZeroWhenNotFound(t *testing.T) {
	wrapped := sanitize.NewSanitizingScrapper(reject.NewRejectingScrapper(stubLeaf{}))

	found, ok := scrapper.As[extraCapable](wrapped)
	assert.False(t, ok)
	assert.Nil(t, found)
}

func TestAs_NilScrapperReturnsZero(t *testing.T) {
	found, ok := scrapper.As[extraCapable](nil)
	assert.False(t, ok)
	assert.Nil(t, found)
}

func TestAs_FirstMatchWins(t *testing.T) {
	// When the outermost decorator already satisfies T, As must return it
	// rather than walking further.
	wrapped := sanitize.NewSanitizingScrapper(&extrasLeaf{})

	found, ok := scrapper.As[scrapper.Unwrapper](wrapped)
	if assert.True(t, ok) {
		assert.Same(t, wrapped, found)
	}
}
