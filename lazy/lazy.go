package lazy

import (
	"sync"
	"sync/atomic"
)

type Lazy[T any] interface {
	Get() (T, error)
	Has() bool
}

var _ Lazy[struct{}] = &lazy[struct{}]{}

type lazy[T any] struct {
	// gen is read and cleared inside once only, so that concurrent callers cannot
	// race on it. A Get that peeked at it to decide whether to run once at all was
	// racing with the Get that was clearing it — which is what any concurrent user
	// does, the per-table reads of a metrics scrape included.
	gen  func() (T, error)
	once sync.Once
	// done says whether the value has been generated, for Has to read without
	// synchronising on anything the generator touches.
	done atomic.Bool
	val  T
	err  error
}

func (l *lazy[T]) Get() (T, error) {
	l.once.Do(func() {
		if l.gen != nil {
			l.val, l.err = l.gen()
			l.gen = nil
		}
		l.done.Store(true)
	})
	// once.Do orders every caller behind the one that ran the generator, so val and
	// err are safe to read here.
	return l.val, l.err
}

func (l *lazy[T]) Has() bool {
	return l.done.Load()
}

func New[T any](gen func() (T, error)) Lazy[T] {
	return &lazy[T]{gen: gen}
}
