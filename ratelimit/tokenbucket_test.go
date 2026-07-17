package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestTokenBucket(t *testing.T, limit int, window time.Duration, clock Clock, opts ...Option) *TokenBucket {
	t.Helper()
	opts = append([]Option{WithClock(clock), WithCleanupInterval(0)}, opts...)
	tb, err := NewTokenBucket(limit, window, opts...)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	t.Cleanup(func() { tb.Close() })
	return tb
}

func TestNewTokenBucket_InvalidArgs(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		window time.Duration
		opts   []Option
	}{
		{"zero limit", 0, time.Second, nil},
		{"negative limit", -1, time.Second, nil},
		{"zero window", 3, 0, nil},
		{"negative window", 3, -time.Second, nil},
		{"negative burst", 3, time.Second, []Option{WithBurst(-1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewTokenBucket(tt.limit, tt.window, tt.opts...); err == nil {
				t.Errorf("NewTokenBucket(%d, %v) = nil error, want error", tt.limit, tt.window)
			}
		})
	}
}

// 新しい key はバケツ満タンから始まり、burst(デフォルトは limit)回まで
// 一気に使える。
func TestTokenBucket_InitialBurst(t *testing.T) {
	clock := newFakeClock()
	tb := newTestTokenBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		res, err := tb.Allow(ctx, "user1")
		if err != nil {
			t.Fatalf("Allow #%d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("Allow #%d: got denied, want allowed", i+1)
		}
		if want := 10 - (i + 1); res.Remaining != want {
			t.Errorf("Allow #%d: Remaining = %d, want %d", i+1, res.Remaining, want)
		}
	}
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Error("Allow #11: got allowed, want denied")
	}
}

// トークンは一定速度で1個ずつ補充される。ウィンドウ一括リセットではない。
func TestTokenBucket_SteadyRefill(t *testing.T) {
	clock := newFakeClock()
	// 10個/分 = 6秒に1個補充。
	tb := newTestTokenBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		tb.Allow(ctx, "user1")
	}

	// 3秒後: まだ0.5個 → 拒否。
	clock.Advance(3 * time.Second)
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at +3s: got allowed, want denied (only 0.5 tokens)")
	}

	// 6秒後: ちょうど1個 → 1回だけ許可。
	clock.Advance(3 * time.Second)
	if res, _ := tb.Allow(ctx, "user1"); !res.Allowed {
		t.Fatal("at +6s: got denied, want allowed")
	}
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at +6s #2: got allowed, want denied")
	}
}

// 補充はバケツの容量で頭打ちになる。長時間止まっても burst 以上は貯まらない。
func TestTokenBucket_RefillCapsAtBurst(t *testing.T) {
	clock := newFakeClock()
	tb := newTestTokenBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	tb.Allow(ctx, "user1") // バケツを初期化(残9個)
	clock.Advance(24 * time.Hour)

	allowed := 0
	for i := 0; i < 100; i++ {
		if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("after 24h idle: allowed = %d, want 10 (capped at burst)", allowed)
	}
}

// 平均レートとバーストを独立に設定できる(rate 1個/秒、burst 5)。
func TestTokenBucket_CustomBurst(t *testing.T) {
	clock := newFakeClock()
	tb := newTestTokenBucket(t, 1, time.Second, clock, WithBurst(5))
	ctx := context.Background()

	// バーストで5回一気に使える。
	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("initial burst: allowed = %d, want 5", allowed)
	}

	// その後は1秒に1個のペース。
	clock.Advance(time.Second)
	if res, _ := tb.Allow(ctx, "user1"); !res.Allowed {
		t.Error("after 1s: got denied, want allowed")
	}
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Error("after 1s #2: got allowed, want denied")
	}
}

func TestTokenBucket_RetryAfter(t *testing.T) {
	clock := newFakeClock()
	// 6秒に1個補充。
	tb := newTestTokenBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		tb.Allow(ctx, "user1")
	}

	// トークン0個 → 次の1個まで6秒。
	res, _ := tb.Allow(ctx, "user1")
	if res.Allowed {
		t.Fatal("got allowed, want denied")
	}
	if want := 6 * time.Second; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}

	// 3秒経過(残0.5個)なら残り3秒。
	clock.Advance(3 * time.Second)
	res, _ = tb.Allow(ctx, "user1")
	if want := 3 * time.Second; res.RetryAfter != want {
		t.Errorf("RetryAfter after 3s = %v, want %v", res.RetryAfter, want)
	}

	// RetryAfter どおりに待てば必ず通る。
	clock.Advance(res.RetryAfter)
	if res, _ := tb.Allow(ctx, "user1"); !res.Allowed {
		t.Error("after waiting RetryAfter: got denied, want allowed")
	}
}

func TestTokenBucket_KeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	tb := newTestTokenBucket(t, 1, time.Minute, clock)
	ctx := context.Background()

	tb.Allow(ctx, "user1")
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("user1 #2: got allowed, want denied")
	}
	if res, _ := tb.Allow(ctx, "user2"); !res.Allowed {
		t.Error("user2 #1: got denied, want allowed")
	}
}

func TestTokenBucket_ContextCanceled(t *testing.T) {
	clock := newFakeClock()
	tb := newTestTokenBucket(t, 1, time.Minute, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tb.Allow(ctx, "user1"); err == nil {
		t.Error("Allow with canceled context: got nil error, want context.Canceled")
	}
}

func TestTokenBucket_DeleteExpired(t *testing.T) {
	clock := newFakeClock()
	tb := newTestTokenBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	tb.Allow(ctx, "idle") // 残9個。満タンまで6秒
	clock.Advance(6 * time.Second)
	tb.Allow(ctx, "active") // 残9個。まだ満タンでない

	tb.deleteExpired(clock.Now())

	tb.mu.Lock()
	defer tb.mu.Unlock()
	if _, ok := tb.states["idle"]; ok {
		t.Error("refilled key 'idle' was not deleted")
	}
	if _, ok := tb.states["active"]; !ok {
		t.Error("non-full key 'active' was deleted")
	}
}

func TestTokenBucket_ConcurrentAccess(t *testing.T) {
	const (
		limit      = 100
		goroutines = 20
		perG       = 10
	)
	// 補充がテスト中に起きないよう、レートは極端に遅くする。
	tb, err := NewTokenBucket(limit, 24*time.Hour, WithBurst(limit), WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()

	ctx := context.Background()
	var allowed int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := 0
			for i := 0; i < perG; i++ {
				res, err := tb.Allow(ctx, "shared")
				if err != nil {
					t.Errorf("Allow: %v", err)
					return
				}
				if res.Allowed {
					local++
				}
			}
			mu.Lock()
			allowed += int64(local)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if allowed != limit {
		t.Errorf("allowed = %d, want exactly %d", allowed, limit)
	}
}

func BenchmarkTokenBucket_SingleKey(b *testing.B) {
	tb, err := NewTokenBucket(1_000_000_000, time.Second, WithCleanupInterval(0))
	if err != nil {
		b.Fatal(err)
	}
	defer tb.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow(ctx, "bench") //nolint:errcheck
		}
	})
}
