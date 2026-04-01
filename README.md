# go-circuitbreaker

[![CI](https://github.com/philiprehberger/go-circuitbreaker/actions/workflows/ci.yml/badge.svg)](https://github.com/philiprehberger/go-circuitbreaker/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/philiprehberger/go-circuitbreaker.svg)](https://pkg.go.dev/github.com/philiprehberger/go-circuitbreaker)
[![Last updated](https://img.shields.io/github/last-commit/philiprehberger/go-circuitbreaker)](https://github.com/philiprehberger/go-circuitbreaker/commits/main)

Generic circuit breaker for Go. Protect external calls with automatic failure detection and recovery

## Installation

```bash
go get github.com/philiprehberger/go-circuitbreaker
```

## Usage

```go
import (
    "context"
    "time"

    cb "github.com/philiprehberger/go-circuitbreaker"
)

// Create a circuit breaker with custom settings.
b := cb.New[string](
    cb.WithThreshold[string](3),
    cb.WithTimeout[string](10*time.Second),
    cb.WithSuccessThreshold[string](2),
    cb.WithOnStateChange[string](func(from, to cb.State) {
        log.Printf("circuit breaker: %s -> %s", from, to)
    }),
)

// Execute a call through the circuit breaker.
result, err := b.Do(context.Background(), func() (string, error) {
    return http.Get("https://api.example.com/data")
})
if errors.Is(err, cb.ErrCircuitOpen) {
    // Circuit is open — use fallback
}
```

### Fallback

Use `DoWithFallback` to provide a fallback when the circuit is open instead of receiving `ErrCircuitOpen`.

```go
result, err := b.DoWithFallback(ctx,
    func(ctx context.Context) (string, error) {
        return callExternalAPI(ctx)
    },
    func(err error) (string, error) {
        return cachedResponse, nil // serve stale data
    },
)
```

### Stats

Monitor circuit breaker activity with `Stats()`.

```go
s := b.Stats()
fmt.Printf("successes=%d failures=%d trips=%d state=%s\n",
    s.Successes, s.Failures, s.Trips, s.State)
```

### Error Classification

Use `WithIgnoreErrors` to prevent certain errors (e.g., business-logic errors) from counting as failures.

```go
b := cb.New[*http.Response](
    cb.WithThreshold[*http.Response](5),
    cb.WithIgnoreErrors[*http.Response](func(err error) bool {
        // 4xx errors are client errors, not infrastructure failures
        var httpErr *HTTPError
        return errors.As(err, &httpErr) && httpErr.StatusCode < 500
    }),
)
```

### Per-Key Circuit Breakers

Use `KeyedBreaker` when you need independent circuit breakers per endpoint, service, or tenant.

```go
kb := cb.NewKeyed[[]byte](
    cb.WithThreshold[[]byte](5),
    cb.WithTimeout[[]byte](30*time.Second),
)

// Each key gets its own independent breaker.
data, err := kb.Do(ctx, "service-a", func() ([]byte, error) {
    return callServiceA()
})

data, err = kb.Do(ctx, "service-b", func() ([]byte, error) {
    return callServiceB()
})
```

### State Transitions

The circuit breaker has three states:

- **Closed** — All calls pass through. Failures are counted. When failures reach the threshold, the breaker opens.
- **Open** — All calls are rejected with `ErrCircuitOpen`. After the timeout elapses, the breaker transitions to half-open.
- **HalfOpen** — A limited number of test calls are allowed through. If enough succeed, the breaker closes. If any fail, the breaker re-opens.

## API

| Type / Function | Description |
|---|---|
| `State` | Circuit breaker state: `StateClosed`, `StateOpen`, `StateHalfOpen` |
| `Breaker[T]` | Generic circuit breaker |
| `New[T](...Option[T])` | Create a new breaker |
| `Breaker.Do(ctx, fn)` | Execute a function through the breaker |
| `Breaker.DoWithFallback(ctx, fn, fallback)` | Execute with fallback when open |
| `Breaker.State()` | Get the current state |
| `Breaker.Stats()` | Get statistics snapshot |
| `Breaker.Reset()` | Force the breaker to closed state |
| `BreakerStats` | Statistics: Successes, Failures, Trips, ConsecutiveFailures, State |
| `KeyedBreaker[T]` | Per-key circuit breakers |
| `NewKeyed[T](...Option[T])` | Create a new keyed breaker |
| `KeyedBreaker.Do(ctx, key, fn)` | Execute with a per-key breaker |
| `KeyedBreaker.DoWithFallback(ctx, key, fn, fallback)` | Execute with per-key fallback |
| `KeyedBreaker.State(key)` | Get state for a key |
| `KeyedBreaker.Stats(key)` | Get statistics for a key |
| `KeyedBreaker.Reset(key)` | Reset a specific key |
| `KeyedBreaker.ResetAll()` | Reset all keys |
| `WithThreshold[T](n)` | Failures before opening (default 5) |
| `WithSuccessThreshold[T](n)` | Successes in half-open to close (default 2) |
| `WithTimeout[T](d)` | Duration in open before half-open (default 30s) |
| `WithMaxHalfOpen[T](n)` | Max concurrent half-open calls (default 1) |
| `WithOnStateChange[T](fn)` | State transition callback |
| `WithIgnoreErrors[T](pred)` | Errors matching predicate don't count as failures |
| `WithOnTrip[T](fn)` | Callback when circuit opens |
| `WithOnReset[T](fn)` | Callback when circuit closes |
| `ErrCircuitOpen` | Error returned when the breaker is open |

## Development

```bash
go test ./...
go vet ./...
```

## Support

If you find this project useful:

⭐ [Star the repo](https://github.com/philiprehberger/go-circuitbreaker)

🐛 [Report issues](https://github.com/philiprehberger/go-circuitbreaker/issues?q=is%3Aissue+is%3Aopen+label%3Abug)

💡 [Suggest features](https://github.com/philiprehberger/go-circuitbreaker/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement)

❤️ [Sponsor development](https://github.com/sponsors/philiprehberger)

🌐 [All Open Source Projects](https://philiprehberger.com/open-source-packages)

💻 [GitHub Profile](https://github.com/philiprehberger)

🔗 [LinkedIn Profile](https://www.linkedin.com/in/philiprehberger)

## License

[MIT](LICENSE)
