package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errFail = errors.New("fail")

func TestNew(t *testing.T) {
	b := New[int]()
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", b.State())
	}
}

func TestDo_Closed_Success(t *testing.T) {
	b := New[int]()
	v, err := b.Do(context.Background(), func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", b.State())
	}
}

func TestDo_Closed_Failure(t *testing.T) {
	b := New[int](WithThreshold[int](3))
	_, err := b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	if !errors.Is(err, errFail) {
		t.Fatalf("expected errFail, got %v", err)
	}
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after one failure, got %v", b.State())
	}
}

func TestDo_OpensAfterThreshold(t *testing.T) {
	b := New[int](WithThreshold[int](3))
	for i := 0; i < 3; i++ {
		_, _ = b.Do(context.Background(), func() (int, error) {
			return 0, errFail
		})
	}
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %v", b.State())
	}
}

func TestDo_Open_ReturnsError(t *testing.T) {
	b := New[int](WithThreshold[int](1), WithTimeout[int](time.Hour))
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	_, err := b.Do(context.Background(), func() (int, error) {
		return 42, nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestDo_HalfOpen_TransitionFromOpen(t *testing.T) {
	b := New[int](WithThreshold[int](1), WithTimeout[int](10*time.Millisecond))
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %v", b.State())
	}
	time.Sleep(20 * time.Millisecond)
	if b.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", b.State())
	}
}

func TestDo_HalfOpen_CloseOnSuccess(t *testing.T) {
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
		WithSuccessThreshold[int](2),
		WithMaxHalfOpen[int](2),
	)
	// Open the breaker.
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	time.Sleep(20 * time.Millisecond)

	// Two successes in half-open should close it.
	for i := 0; i < 2; i++ {
		_, err := b.Do(context.Background(), func() (int, error) {
			return 1, nil
		})
		if err != nil {
			t.Fatalf("unexpected error on half-open call %d: %v", i, err)
		}
	}
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after successes in half-open, got %v", b.State())
	}
}

func TestDo_HalfOpen_ReopenOnFailure(t *testing.T) {
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
	)
	// Open the breaker.
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	time.Sleep(20 * time.Millisecond)

	// Failure in half-open should re-open.
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen after half-open failure, got %v", b.State())
	}
}

func TestOnStateChange(t *testing.T) {
	var transitions []struct{ from, to State }
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
		WithSuccessThreshold[int](1),
		WithOnStateChange[int](func(from, to State) {
			transitions = append(transitions, struct{ from, to State }{from, to})
		}),
	)

	// Closed -> Open
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})

	// Wait for half-open
	time.Sleep(20 * time.Millisecond)
	_ = b.State() // triggers Open -> HalfOpen

	// HalfOpen -> Closed
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 1, nil
	})

	if len(transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %d: %+v", len(transitions), transitions)
	}
	if transitions[0].from != StateClosed || transitions[0].to != StateOpen {
		t.Fatalf("transition 0: expected Closed->Open, got %v->%v", transitions[0].from, transitions[0].to)
	}
	if transitions[1].from != StateOpen || transitions[1].to != StateHalfOpen {
		t.Fatalf("transition 1: expected Open->HalfOpen, got %v->%v", transitions[1].from, transitions[1].to)
	}
	if transitions[2].from != StateHalfOpen || transitions[2].to != StateClosed {
		t.Fatalf("transition 2: expected HalfOpen->Closed, got %v->%v", transitions[2].from, transitions[2].to)
	}
}

func TestReset(t *testing.T) {
	b := New[int](WithThreshold[int](1))
	_, _ = b.Do(context.Background(), func() (int, error) {
		return 0, errFail
	})
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %v", b.State())
	}
	b.Reset()
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after reset, got %v", b.State())
	}
}

func TestConcurrentAccess(t *testing.T) {
	b := New[int](WithThreshold[int](100))
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = b.Do(context.Background(), func() (int, error) {
					return 1, nil
				})
			}
		}()
	}
	wg.Wait()
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", b.State())
	}
}

func TestStats_Counting(t *testing.T) {
	b := New[int](WithThreshold[int](5))
	ctx := context.Background()

	// 3 successes
	for i := 0; i < 3; i++ {
		_, _ = b.Do(ctx, func() (int, error) {
			return 1, nil
		})
	}

	// 2 failures
	for i := 0; i < 2; i++ {
		_, _ = b.Do(ctx, func() (int, error) {
			return 0, errFail
		})
	}

	s := b.Stats()
	if s.Successes != 3 {
		t.Fatalf("expected 3 successes, got %d", s.Successes)
	}
	if s.Failures != 2 {
		t.Fatalf("expected 2 failures, got %d", s.Failures)
	}
	if s.ConsecutiveFailures != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", s.ConsecutiveFailures)
	}
	if s.Trips != 0 {
		t.Fatalf("expected 0 trips, got %d", s.Trips)
	}
	if s.State != StateClosed {
		t.Fatalf("expected StateClosed, got %v", s.State)
	}
}

func TestStats_Trips(t *testing.T) {
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
		WithSuccessThreshold[int](1),
	)
	ctx := context.Background()

	// Trip 1
	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	// Wait for half-open, then close
	time.Sleep(20 * time.Millisecond)
	_, _ = b.Do(ctx, func() (int, error) {
		return 1, nil
	})

	// Trip 2
	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	s := b.Stats()
	if s.Trips != 2 {
		t.Fatalf("expected 2 trips, got %d", s.Trips)
	}
}

func TestStats_ConcurrentCounting(t *testing.T) {
	b := New[int](WithThreshold[int](10000))
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = b.Do(context.Background(), func() (int, error) {
					return 1, nil
				})
			}
		}()
	}
	wg.Wait()

	s := b.Stats()
	if s.Successes != 5000 {
		t.Fatalf("expected 5000 successes, got %d", s.Successes)
	}
}

func TestDoWithFallback_Open(t *testing.T) {
	b := New[string](WithThreshold[string](1), WithTimeout[string](time.Hour))
	ctx := context.Background()

	// Open the breaker
	_, _ = b.DoWithFallback(ctx,
		func(ctx context.Context) (string, error) {
			return "", errFail
		},
		func(err error) (string, error) {
			return "fallback", nil
		},
	)

	// Now circuit is open — fallback should fire
	result, err := b.DoWithFallback(ctx,
		func(ctx context.Context) (string, error) {
			return "primary", nil
		},
		func(err error) (string, error) {
			if !errors.Is(err, ErrCircuitOpen) {
				t.Fatalf("fallback expected ErrCircuitOpen, got %v", err)
			}
			return "fallback", nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "fallback" {
		t.Fatalf("expected 'fallback', got %q", result)
	}
}

func TestDoWithFallback_Closed(t *testing.T) {
	b := New[string]()
	ctx := context.Background()

	result, err := b.DoWithFallback(ctx,
		func(ctx context.Context) (string, error) {
			return "primary", nil
		},
		func(err error) (string, error) {
			return "fallback", nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "primary" {
		t.Fatalf("expected 'primary', got %q", result)
	}
}

func TestDoWithFallback_FallbackError(t *testing.T) {
	b := New[string](WithThreshold[string](1), WithTimeout[string](time.Hour))
	ctx := context.Background()

	// Open the breaker
	_, _ = b.Do(ctx, func() (string, error) {
		return "", errFail
	})

	fallbackErr := errors.New("fallback failed")
	_, err := b.DoWithFallback(ctx,
		func(ctx context.Context) (string, error) {
			return "primary", nil
		},
		func(err error) (string, error) {
			return "", fallbackErr
		},
	)
	if !errors.Is(err, fallbackErr) {
		t.Fatalf("expected fallbackErr, got %v", err)
	}
}

func TestDoWithFallback_ContextCanceled(t *testing.T) {
	b := New[string]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.DoWithFallback(ctx,
		func(ctx context.Context) (string, error) {
			return "primary", nil
		},
		func(err error) (string, error) {
			return "fallback", nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

var errBusiness = errors.New("business error")

func TestWithIgnoreErrors_ClosedState(t *testing.T) {
	b := New[int](
		WithThreshold[int](2),
		WithIgnoreErrors[int](func(err error) bool {
			return errors.Is(err, errBusiness)
		}),
	)
	ctx := context.Background()

	// Business errors should not count as failures
	for i := 0; i < 5; i++ {
		_, err := b.Do(ctx, func() (int, error) {
			return 0, errBusiness
		})
		if !errors.Is(err, errBusiness) {
			t.Fatalf("expected errBusiness, got %v", err)
		}
	}

	// Should still be closed
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after ignored errors, got %v", b.State())
	}

	s := b.Stats()
	if s.Successes != 5 {
		t.Fatalf("ignored errors should count as successes, got %d", s.Successes)
	}
	if s.Failures != 0 {
		t.Fatalf("expected 0 failures, got %d", s.Failures)
	}
}

func TestWithIgnoreErrors_NonIgnoredStillTrips(t *testing.T) {
	b := New[int](
		WithThreshold[int](2),
		WithIgnoreErrors[int](func(err error) bool {
			return errors.Is(err, errBusiness)
		}),
	)
	ctx := context.Background()

	// Non-ignored errors should still count
	for i := 0; i < 2; i++ {
		_, _ = b.Do(ctx, func() (int, error) {
			return 0, errFail
		})
	}

	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen after non-ignored errors, got %v", b.State())
	}
}

func TestWithIgnoreErrors_HalfOpenState(t *testing.T) {
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
		WithSuccessThreshold[int](2),
		WithMaxHalfOpen[int](5),
		WithIgnoreErrors[int](func(err error) bool {
			return errors.Is(err, errBusiness)
		}),
	)
	ctx := context.Background()

	// Open the breaker with a real error
	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	time.Sleep(20 * time.Millisecond)

	// Ignored errors in half-open should count as successes toward closing
	for i := 0; i < 2; i++ {
		_, _ = b.Do(ctx, func() (int, error) {
			return 0, errBusiness
		})
	}

	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after ignored errors in half-open, got %v", b.State())
	}
}

func TestWithOnTrip(t *testing.T) {
	var tripCount atomic.Int32
	b := New[int](
		WithThreshold[int](2),
		WithOnTrip[int](func() {
			tripCount.Add(1)
		}),
	)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		_, _ = b.Do(ctx, func() (int, error) {
			return 0, errFail
		})
	}

	if tripCount.Load() != 1 {
		t.Fatalf("expected onTrip called once, got %d", tripCount.Load())
	}
}

func TestWithOnReset(t *testing.T) {
	var resetCount atomic.Int32
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
		WithSuccessThreshold[int](1),
		WithOnReset[int](func() {
			resetCount.Add(1)
		}),
	)
	ctx := context.Background()

	// Trip the breaker
	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	time.Sleep(20 * time.Millisecond)

	// Recover
	_, _ = b.Do(ctx, func() (int, error) {
		return 1, nil
	})

	if resetCount.Load() != 1 {
		t.Fatalf("expected onReset called once, got %d", resetCount.Load())
	}
}

func TestWithOnReset_ManualReset(t *testing.T) {
	var resetCount atomic.Int32
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](time.Hour),
		WithOnReset[int](func() {
			resetCount.Add(1)
		}),
	)
	ctx := context.Background()

	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	b.Reset()

	if resetCount.Load() != 1 {
		t.Fatalf("expected onReset called once on manual reset, got %d", resetCount.Load())
	}
}

func TestWithOnTrip_And_OnReset_Together(t *testing.T) {
	var tripCount, resetCount atomic.Int32
	b := New[int](
		WithThreshold[int](1),
		WithTimeout[int](10*time.Millisecond),
		WithSuccessThreshold[int](1),
		WithOnTrip[int](func() {
			tripCount.Add(1)
		}),
		WithOnReset[int](func() {
			resetCount.Add(1)
		}),
	)
	ctx := context.Background()

	// Trip
	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	time.Sleep(20 * time.Millisecond)

	// Reset via success
	_, _ = b.Do(ctx, func() (int, error) {
		return 1, nil
	})

	// Trip again
	_, _ = b.Do(ctx, func() (int, error) {
		return 0, errFail
	})

	if tripCount.Load() != 2 {
		t.Fatalf("expected 2 trips, got %d", tripCount.Load())
	}
	if resetCount.Load() != 1 {
		t.Fatalf("expected 1 reset, got %d", resetCount.Load())
	}
}
