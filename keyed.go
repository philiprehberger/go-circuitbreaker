package circuitbreaker

import (
	"context"
	"sync"
)

// KeyedBreaker manages independent circuit breakers for different keys. Each key
// gets its own Breaker instance, lazily initialized on first use. It is safe for
// concurrent use.
type KeyedBreaker[T any] struct {
	breakers sync.Map
	opts     []Option[T]
}

// NewKeyed creates a new KeyedBreaker. The provided options are applied to each
// per-key Breaker when it is created.
func NewKeyed[T any](opts ...Option[T]) *KeyedBreaker[T] {
	return &KeyedBreaker[T]{opts: opts}
}

// getOrCreate returns the Breaker for the given key, creating one if it does not
// exist.
func (kb *KeyedBreaker[T]) getOrCreate(key string) *Breaker[T] {
	if v, ok := kb.breakers.Load(key); ok {
		return v.(*Breaker[T])
	}
	b := New[T](kb.opts...)
	actual, _ := kb.breakers.LoadOrStore(key, b)
	return actual.(*Breaker[T])
}

// Do executes fn within the circuit breaker associated with the given key. Each
// key has an independent breaker that tracks its own failure and success counts.
func (kb *KeyedBreaker[T]) Do(ctx context.Context, key string, fn func() (T, error)) (T, error) {
	return kb.getOrCreate(key).Do(ctx, fn)
}

// State returns the current state of the circuit breaker for the given key. If
// no breaker exists for the key, it returns StateClosed.
func (kb *KeyedBreaker[T]) State(key string) State {
	if v, ok := kb.breakers.Load(key); ok {
		return v.(*Breaker[T]).State()
	}
	return StateClosed
}

// Reset resets the circuit breaker for the given key back to the closed state.
// If no breaker exists for the key, this is a no-op.
func (kb *KeyedBreaker[T]) Reset(key string) {
	if v, ok := kb.breakers.Load(key); ok {
		v.(*Breaker[T]).Reset()
	}
}

// ResetAll removes all per-key circuit breakers, effectively resetting
// everything.
func (kb *KeyedBreaker[T]) ResetAll() {
	kb.breakers.Range(func(key, _ any) bool {
		kb.breakers.Delete(key)
		return true
	})
}

// Stats returns the statistics for the circuit breaker associated with the
// given key. If no breaker exists for the key, it returns a zero-value
// BreakerStats with StateClosed.
func (kb *KeyedBreaker[T]) Stats(key string) BreakerStats {
	if v, ok := kb.breakers.Load(key); ok {
		return v.(*Breaker[T]).Stats()
	}
	return BreakerStats{State: StateClosed}
}

// DoWithFallback executes fn within the circuit breaker associated with the
// given key. If the circuit is open, it calls the fallback function instead of
// returning ErrCircuitOpen.
func (kb *KeyedBreaker[T]) DoWithFallback(ctx context.Context, key string, fn func(context.Context) (T, error), fallback func(error) (T, error)) (T, error) {
	return kb.getOrCreate(key).DoWithFallback(ctx, fn, fallback)
}
