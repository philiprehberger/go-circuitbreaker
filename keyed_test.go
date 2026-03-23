package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestKeyedDo(t *testing.T) {
	kb := NewKeyed[int](WithThreshold[int](2), WithTimeout[int](time.Hour))
	ctx := context.Background()

	// Fail key "a" twice to open it.
	for i := 0; i < 2; i++ {
		_, _ = kb.Do(ctx, "a", func() (int, error) {
			return 0, errFail
		})
	}

	// Key "a" should be open.
	_, err := kb.Do(ctx, "a", func() (int, error) {
		return 1, nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen for key 'a', got %v", err)
	}

	// Key "b" should still work.
	v, err := kb.Do(ctx, "b", func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error for key 'b': %v", err)
	}
	if v != 42 {
		t.Fatalf("expected 42 for key 'b', got %d", v)
	}
}

func TestKeyedState(t *testing.T) {
	kb := NewKeyed[int](WithThreshold[int](1), WithTimeout[int](time.Hour))
	ctx := context.Background()

	// Unknown key should return StateClosed.
	if kb.State("x") != StateClosed {
		t.Fatalf("expected StateClosed for unknown key, got %v", kb.State("x"))
	}

	// Open key "a".
	_, _ = kb.Do(ctx, "a", func() (int, error) {
		return 0, errFail
	})
	if kb.State("a") != StateOpen {
		t.Fatalf("expected StateOpen for key 'a', got %v", kb.State("a"))
	}
	if kb.State("b") != StateClosed {
		t.Fatalf("expected StateClosed for key 'b', got %v", kb.State("b"))
	}
}

func TestKeyedReset(t *testing.T) {
	kb := NewKeyed[int](WithThreshold[int](1), WithTimeout[int](time.Hour))
	ctx := context.Background()

	_, _ = kb.Do(ctx, "a", func() (int, error) {
		return 0, errFail
	})
	if kb.State("a") != StateOpen {
		t.Fatalf("expected StateOpen, got %v", kb.State("a"))
	}

	kb.Reset("a")
	if kb.State("a") != StateClosed {
		t.Fatalf("expected StateClosed after reset, got %v", kb.State("a"))
	}
}

func TestKeyedResetAll(t *testing.T) {
	kb := NewKeyed[int](WithThreshold[int](1), WithTimeout[int](time.Hour))
	ctx := context.Background()

	_, _ = kb.Do(ctx, "a", func() (int, error) {
		return 0, errFail
	})
	_, _ = kb.Do(ctx, "b", func() (int, error) {
		return 0, errFail
	})

	kb.ResetAll()

	if kb.State("a") != StateClosed {
		t.Fatalf("expected StateClosed for key 'a' after ResetAll, got %v", kb.State("a"))
	}
	if kb.State("b") != StateClosed {
		t.Fatalf("expected StateClosed for key 'b' after ResetAll, got %v", kb.State("b"))
	}
}

func TestKeyedStats(t *testing.T) {
	kb := NewKeyed[int](WithThreshold[int](5))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, _ = kb.Do(ctx, "a", func() (int, error) {
			return 1, nil
		})
	}
	_, _ = kb.Do(ctx, "a", func() (int, error) {
		return 0, errFail
	})

	s := kb.Stats("a")
	if s.Successes != 3 {
		t.Fatalf("expected 3 successes for key 'a', got %d", s.Successes)
	}
	if s.Failures != 1 {
		t.Fatalf("expected 1 failure for key 'a', got %d", s.Failures)
	}

	// Unknown key should return zero stats
	s2 := kb.Stats("unknown")
	if s2.Successes != 0 || s2.Failures != 0 || s2.State != StateClosed {
		t.Fatalf("expected zero stats for unknown key, got %+v", s2)
	}
}

func TestKeyedDoWithFallback(t *testing.T) {
	kb := NewKeyed[string](WithThreshold[string](1), WithTimeout[string](time.Hour))
	ctx := context.Background()

	// Open key "a"
	_, _ = kb.Do(ctx, "a", func() (string, error) {
		return "", errFail
	})

	// Fallback should fire for open key
	result, err := kb.DoWithFallback(ctx, "a",
		func(ctx context.Context) (string, error) {
			return "primary", nil
		},
		func(err error) (string, error) {
			return fmt.Sprintf("fallback: %v", err), nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "fallback: circuit breaker is open" {
		t.Fatalf("expected fallback result, got %q", result)
	}

	// Key "b" should use primary
	result, err = kb.DoWithFallback(ctx, "b",
		func(ctx context.Context) (string, error) {
			return "primary-b", nil
		},
		func(err error) (string, error) {
			return "fallback-b", nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "primary-b" {
		t.Fatalf("expected 'primary-b', got %q", result)
	}
}
