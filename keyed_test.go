package circuitbreaker

import (
	"context"
	"errors"
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
