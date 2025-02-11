package lazy

import "sync"

type Lazy[T any] interface {
	Get() (T, error)
	Has() bool
}

var _ Lazy[struct{}] = &lazy[struct{}]{}

type lazy[T any] struct {
	gen  func() (T, error)
	once sync.Once
	val  T
	err  error
}

func (l *lazy[T]) Get() (T, error) {
	if l.gen != nil {
		l.once.Do(func() {
			l.val, l.err = l.gen()
			l.gen = nil
		})
	}
	return l.val, l.err
}

func (l *lazy[T]) Has() bool {
	return l.gen == nil
}

func New[T any](gen func() (T, error)) Lazy[T] {
	return &lazy[T]{gen: gen}
}
