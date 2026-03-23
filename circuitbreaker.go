// Package circuitbreaker provides a circuit breaker pattern implementation for
// external calls. It monitors failures and automatically prevents calls to
// failing services, allowing them time to recover.
package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrCircuitOpen is returned when a call is attempted on an open circuit breaker.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// State represents the current state of a circuit breaker.
type State int

const (
	// StateClosed allows all calls through and counts failures.
	StateClosed State = iota
	// StateOpen rejects all calls immediately.
	StateOpen
	// StateHalfOpen allows a limited number of test calls through.
	StateHalfOpen
)

// String returns a human-readable representation of the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// BreakerStats contains point-in-time statistics for a circuit breaker.
type BreakerStats struct {
	Successes           int64
	Failures            int64
	Trips               int64
	ConsecutiveFailures int
	State               State
}

// Option is a functional option for configuring a Breaker.
type Option[T any] func(*config)

type config struct {
	threshold        int
	successThreshold int
	timeout          time.Duration
	maxHalfOpen      int
	onStateChange    func(from, to State)
	ignoreErrors     func(error) bool
	onTrip           func()
	onReset          func()
}

func defaultConfig() config {
	return config{
		threshold:        5,
		successThreshold: 2,
		timeout:          30 * time.Second,
		maxHalfOpen:      1,
	}
}

// WithThreshold sets the number of consecutive failures required to open the
// circuit breaker. Default is 5.
func WithThreshold[T any](n int) Option[T] {
	return func(c *config) {
		if n > 0 {
			c.threshold = n
		}
	}
}

// WithSuccessThreshold sets the number of consecutive successes in the half-open
// state required to close the circuit breaker. Default is 2.
func WithSuccessThreshold[T any](n int) Option[T] {
	return func(c *config) {
		if n > 0 {
			c.successThreshold = n
		}
	}
}

// WithTimeout sets the duration the circuit breaker stays open before
// transitioning to half-open. Default is 30 seconds.
func WithTimeout[T any](d time.Duration) Option[T] {
	return func(c *config) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithOnStateChange sets a callback that is invoked whenever the circuit breaker
// transitions between states.
func WithOnStateChange[T any](fn func(from, to State)) Option[T] {
	return func(c *config) {
		c.onStateChange = fn
	}
}

// WithMaxHalfOpen sets the maximum number of concurrent calls allowed in the
// half-open state. Default is 1.
func WithMaxHalfOpen[T any](n int) Option[T] {
	return func(c *config) {
		if n > 0 {
			c.maxHalfOpen = n
		}
	}
}

// WithIgnoreErrors sets a predicate that determines which errors should not
// count as failures. If pred returns true for an error, that error is treated
// as a success from the circuit breaker's perspective (the error is still
// returned to the caller). This is useful for distinguishing business-logic
// errors from infrastructure errors.
func WithIgnoreErrors[T any](pred func(error) bool) Option[T] {
	return func(c *config) {
		c.ignoreErrors = pred
	}
}

// WithOnTrip sets a callback that is invoked when the circuit breaker
// transitions to the open state (trips).
func WithOnTrip[T any](fn func()) Option[T] {
	return func(c *config) {
		c.onTrip = fn
	}
}

// WithOnReset sets a callback that is invoked when the circuit breaker
// transitions back to the closed state.
func WithOnReset[T any](fn func()) Option[T] {
	return func(c *config) {
		c.onReset = fn
	}
}

// Breaker is a generic circuit breaker that wraps calls to external services.
// It is safe for concurrent use.
type Breaker[T any] struct {
	mu              sync.Mutex
	cfg             config
	state           State
	failureCount    int
	successCount    int
	halfOpenCount   int
	openedAt        time.Time
	totalSuccesses  atomic.Int64
	totalFailures   atomic.Int64
	totalTrips      atomic.Int64
}

// New creates a new Breaker with the given options.
func New[T any](opts ...Option[T]) *Breaker[T] {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Breaker[T]{cfg: cfg}
}

// State returns the current state of the circuit breaker.
func (b *Breaker[T]) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentState()
}

// currentState returns the state, performing the open-to-half-open transition
// if the timeout has elapsed. Must be called with mu held.
func (b *Breaker[T]) currentState() State {
	if b.state == StateOpen && time.Since(b.openedAt) >= b.cfg.timeout {
		b.setState(StateHalfOpen)
	}
	return b.state
}

// setState transitions to a new state and invokes the state change hook if set.
// Must be called with mu held.
func (b *Breaker[T]) setState(to State) {
	from := b.state
	if from == to {
		return
	}
	b.state = to
	b.failureCount = 0
	b.successCount = 0
	b.halfOpenCount = 0
	if to == StateOpen {
		b.openedAt = time.Now()
		b.totalTrips.Add(1)
		if b.cfg.onTrip != nil {
			b.cfg.onTrip()
		}
	}
	if to == StateClosed && from != StateClosed {
		if b.cfg.onReset != nil {
			b.cfg.onReset()
		}
	}
	if b.cfg.onStateChange != nil {
		b.cfg.onStateChange(from, to)
	}
}

// Do executes fn within the circuit breaker. If the circuit is open, it returns
// ErrCircuitOpen without calling fn. In the half-open state, only a limited
// number of concurrent calls are allowed through.
func (b *Breaker[T]) Do(ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T

	if err := ctx.Err(); err != nil {
		return zero, err
	}

	b.mu.Lock()
	state := b.currentState()

	switch state {
	case StateClosed:
		b.mu.Unlock()
		return b.executeClosed(fn)
	case StateOpen:
		b.mu.Unlock()
		return zero, ErrCircuitOpen
	case StateHalfOpen:
		if b.halfOpenCount >= b.cfg.maxHalfOpen {
			b.mu.Unlock()
			return zero, ErrCircuitOpen
		}
		b.halfOpenCount++
		b.mu.Unlock()
		return b.executeHalfOpen(fn)
	default:
		b.mu.Unlock()
		return zero, ErrCircuitOpen
	}
}

// executeClosed runs fn in the closed state, tracking failures.
func (b *Breaker[T]) executeClosed(fn func() (T, error)) (T, error) {
	result, err := fn()
	b.mu.Lock()
	defer b.mu.Unlock()

	if err != nil {
		if b.cfg.ignoreErrors != nil && b.cfg.ignoreErrors(err) {
			b.totalSuccesses.Add(1)
			b.failureCount = 0
			return result, err
		}
		b.totalFailures.Add(1)
		b.failureCount++
		if b.failureCount >= b.cfg.threshold {
			b.setState(StateOpen)
		}
		return result, err
	}

	b.totalSuccesses.Add(1)
	b.failureCount = 0
	return result, nil
}

// executeHalfOpen runs fn in the half-open state, counting successes and
// re-opening on failure.
func (b *Breaker[T]) executeHalfOpen(fn func() (T, error)) (T, error) {
	result, err := fn()
	b.mu.Lock()
	defer b.mu.Unlock()

	if err != nil {
		if b.cfg.ignoreErrors != nil && b.cfg.ignoreErrors(err) {
			b.totalSuccesses.Add(1)
			b.successCount++
			if b.successCount >= b.cfg.successThreshold {
				b.setState(StateClosed)
			}
			return result, err
		}
		b.totalFailures.Add(1)
		b.setState(StateOpen)
		return result, err
	}

	b.totalSuccesses.Add(1)
	b.successCount++
	if b.successCount >= b.cfg.successThreshold {
		b.setState(StateClosed)
	}
	return result, nil
}

// Reset forces the circuit breaker back to the closed state, clearing all
// counters.
func (b *Breaker[T]) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setState(StateClosed)
}

// Stats returns a point-in-time snapshot of the circuit breaker's statistics.
func (b *Breaker[T]) Stats() BreakerStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BreakerStats{
		Successes:           b.totalSuccesses.Load(),
		Failures:            b.totalFailures.Load(),
		Trips:               b.totalTrips.Load(),
		ConsecutiveFailures: b.failureCount,
		State:               b.currentState(),
	}
}

// DoWithFallback executes fn within the circuit breaker. If the circuit is open,
// it calls the fallback function instead of returning ErrCircuitOpen. The
// fallback receives ErrCircuitOpen as its argument.
func (b *Breaker[T]) DoWithFallback(ctx context.Context, fn func(context.Context) (T, error), fallback func(error) (T, error)) (T, error) {
	var zero T

	if err := ctx.Err(); err != nil {
		return zero, err
	}

	b.mu.Lock()
	state := b.currentState()

	switch state {
	case StateClosed:
		b.mu.Unlock()
		return b.executeClosed(func() (T, error) {
			return fn(ctx)
		})
	case StateOpen:
		b.mu.Unlock()
		return fallback(ErrCircuitOpen)
	case StateHalfOpen:
		if b.halfOpenCount >= b.cfg.maxHalfOpen {
			b.mu.Unlock()
			return fallback(ErrCircuitOpen)
		}
		b.halfOpenCount++
		b.mu.Unlock()
		return b.executeHalfOpen(func() (T, error) {
			return fn(ctx)
		})
	default:
		b.mu.Unlock()
		return fallback(ErrCircuitOpen)
	}
}
