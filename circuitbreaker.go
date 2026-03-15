// Package circuitbreaker provides a circuit breaker pattern implementation for
// external calls. It monitors failures and automatically prevents calls to
// failing services, allowing them time to recover.
package circuitbreaker

import (
	"context"
	"errors"
	"sync"
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

// Option is a functional option for configuring a Breaker.
type Option[T any] func(*config)

type config struct {
	threshold        int
	successThreshold int
	timeout          time.Duration
	maxHalfOpen      int
	onStateChange    func(from, to State)
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
		b.failureCount++
		if b.failureCount >= b.cfg.threshold {
			b.setState(StateOpen)
		}
		return result, err
	}

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
		b.setState(StateOpen)
		return result, err
	}

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
