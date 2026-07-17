package middleware

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/repenguin22/go-rate-limit-practice/ratelimit"
)

// stubLimiter は判定結果を固定で返すテスト用リミッター。
type stubLimiter struct {
	res     ratelimit.Result
	err     error
	gotKeys []string
}

func (s *stubLimiter) Allow(_ context.Context, key string) (ratelimit.Result, error) {
	s.gotKeys = append(s.gotKeys, key)
	return s.res, s.err
}

// okHandler はミドルウェアを通過したことを記録するハンドラー。
func okHandler(called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called = true
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok") //nolint:errcheck
	})
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNew_InvalidArgs(t *testing.T) {
	if _, err := New(nil, KeyByIP); err == nil {
		t.Error("New(nil limiter) = nil error, want error")
	}
	if _, err := New(&stubLimiter{}, nil); err == nil {
		t.Error("New(nil key func) = nil error, want error")
	}
}

func TestWrap_AllowedRequestPassesWithHeaders(t *testing.T) {
	resetAt := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	lim := &stubLimiter{res: ratelimit.Result{
		Allowed: true, Limit: 10, Remaining: 7, ResetAt: resetAt,
	}}
	m, err := New(lim, KeyByIP, WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}

	var called bool
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	m.Wrap(okHandler(&called)).ServeHTTP(rec, req)

	if !called {
		t.Fatal("next handler was not called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "10" {
		t.Errorf("X-RateLimit-Limit = %q, want %q", got, "10")
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "7" {
		t.Errorf("X-RateLimit-Remaining = %q, want %q", got, "7")
	}
	if got := rec.Header().Get("X-RateLimit-Reset"); got != "1767225660" {
		t.Errorf("X-RateLimit-Reset = %q, want %q", got, "1767225660")
	}
}

func TestWrap_DeniedRequestGets429(t *testing.T) {
	lim := &stubLimiter{res: ratelimit.Result{
		Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 2500 * time.Millisecond,
	}}
	m, err := New(lim, KeyByIP, WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}

	var called bool
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	m.Wrap(okHandler(&called)).ServeHTTP(rec, req)

	if called {
		t.Fatal("next handler was called despite denial")
	}
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	// 2.5秒 → 切り上げて3秒(切り捨てると待ち足りない案内になる)。
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After = %q, want %q", got, "3")
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want %q", got, "0")
	}
}

func TestWrap_FailOpenPassesOnLimiterError(t *testing.T) {
	lim := &stubLimiter{err: errors.New("redis: connection refused")}
	m, err := New(lim, KeyByIP, WithLogger(discardLogger())) // デフォルトは FailOpen
	if err != nil {
		t.Fatal(err)
	}

	var called bool
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	m.Wrap(okHandler(&called)).ServeHTTP(rec, req)

	if !called {
		t.Fatal("FailOpen: next handler was not called on limiter error")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestWrap_FailCloseRejectsOnLimiterError(t *testing.T) {
	lim := &stubLimiter{err: errors.New("redis: connection refused")}
	m, err := New(lim, KeyByIP, WithLogger(discardLogger()), WithFailurePolicy(FailClose))
	if err != nil {
		t.Fatal(err)
	}

	var called bool
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	m.Wrap(okHandler(&called)).ServeHTTP(rec, req)

	if called {
		t.Fatal("FailClose: next handler was called on limiter error")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

func TestWrap_KeyErrorRejectsWith400(t *testing.T) {
	lim := &stubLimiter{res: ratelimit.Result{Allowed: true}}
	m, err := New(lim, KeyByHeader("X-API-Key"), WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}

	var called bool
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil) // X-API-Key なし
	m.Wrap(okHandler(&called)).ServeHTTP(rec, req)

	if called {
		t.Fatal("next handler was called despite missing key")
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if len(lim.gotKeys) != 0 {
		t.Errorf("limiter was consulted despite key error: %v", lim.gotKeys)
	}
}

func TestKeyByIP_StripsPort(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:51234"
	key, err := KeyByIP(req)
	if err != nil {
		t.Fatal(err)
	}
	if key != "203.0.113.7" {
		t.Errorf("key = %q, want %q", key, "203.0.113.7")
	}
}

func TestKeyByHeader_UsesHeaderValue(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "key-abc")
	key, err := KeyByHeader("X-API-Key")(req)
	if err != nil {
		t.Fatal(err)
	}
	if key != "key-abc" {
		t.Errorf("key = %q, want %q", key, "key-abc")
	}
}

// TestWrap_EndToEndWithRealLimiter は本物のリミッターと組み合わせた結合テスト。
// 同一IPは制限され、別IPは影響を受けないことを HTTP 越しに確認する。
func TestWrap_EndToEndWithRealLimiter(t *testing.T) {
	fw, err := ratelimit.NewFixedWindow(2, time.Hour, ratelimit.WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

	m, err := New(fw, KeyByIP, WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}
	var called bool
	h := m.Wrap(okHandler(&called))

	do := func(ip string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = ip + ":12345"
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := do("10.0.0.1"); code != http.StatusOK {
		t.Fatalf("req1: status = %d, want 200", code)
	}
	if code := do("10.0.0.1"); code != http.StatusOK {
		t.Fatalf("req2: status = %d, want 200", code)
	}
	if code := do("10.0.0.1"); code != http.StatusTooManyRequests {
		t.Fatalf("req3: status = %d, want 429", code)
	}
	// 別IPは独立してカウントされる。
	if code := do("10.0.0.2"); code != http.StatusOK {
		t.Fatalf("other ip: status = %d, want 200", code)
	}
}
