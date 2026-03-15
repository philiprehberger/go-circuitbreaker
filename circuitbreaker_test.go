package circuitbreaker

import (
	"context"
	"errors"
	"sync"
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
