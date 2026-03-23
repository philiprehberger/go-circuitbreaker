# Changelog

## 0.2.0

- Add `Stats()` method with atomic counters for successes, failures, and trips
- Add `DoWithFallback` method for providing fallback behavior when circuit is open
- Add `WithIgnoreErrors` option to exclude specific errors from failure counting
- Add `WithOnTrip` callback option for circuit open transitions
- Add `WithOnReset` callback option for circuit close transitions
- Add `Stats` and `DoWithFallback` to `KeyedBreaker`

## 0.1.3

- Consolidate README badges onto single line, fix CHANGELOG format

## 0.1.2

- Add Development section to README

## 0.1.0

- Initial release
- Generic circuit breaker with three states
- Per-key circuit breakers
- Configurable thresholds and timeouts
- State transition hooks
