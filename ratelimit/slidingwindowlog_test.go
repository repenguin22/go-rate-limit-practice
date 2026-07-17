package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestSlidingWindowLog(t *testing.T, limit int, window time.Duration, clock Clock) *SlidingWindowLog {
	t.Helper()
	sl, err := NewSlidingWindowLog(limit, window, WithClock(clock), WithCleanupInterval(0))
	if err != nil {
		t.Fatalf("NewSlidingWindowLog: %v", err)
	}
	t.Cleanup(func() { sl.Close() })
	return sl
}

func TestNewSlidingWindowLog_InvalidArgs(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		window time.Duration
	}{
		{"zero limit", 0, time.Second},
		{"negative limit", -1, time.Second},
		{"zero window", 3, 0},
		{"negative window", 3, -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSlidingWindowLog(tt.limit, tt.window); err == nil {
				t.Errorf("NewSlidingWindowLog(%d, %v) = nil error, want error", tt.limit, tt.window)
			}
		})
	}
}

func TestSlidingWindowLog_AllowUpToLimit(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 3, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := sl.Allow(ctx, "user1")
		if err != nil {
			t.Fatalf("Allow #%d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("Allow #%d: got denied, want allowed", i+1)
		}
		if want := 3 - (i + 1); res.Remaining != want {
			t.Errorf("Allow #%d: Remaining = %d, want %d", i+1, res.Remaining, want)
		}
	}

	res, _ := sl.Allow(ctx, "user1")
	if res.Allowed {
		t.Error("Allow #4: got allowed, want denied")
	}
	if res.RetryAfter != time.Minute {
		t.Errorf("Allow #4: RetryAfter = %v, want %v", res.RetryAfter, time.Minute)
	}
}

// TestSlidingWindowLog_PartialExpiry は古い記録から順に枠が回復することを検証する。
func TestSlidingWindowLog_PartialExpiry(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 10, time.Minute, clock)
	ctx := context.Background()

	// 00:00:00 に5回、00:00:30 に5回で上限に到達。
	for i := 0; i < 5; i++ {
		sl.Allow(ctx, "user1")
	}
	clock.Advance(30 * time.Second)
	for i := 0; i < 5; i++ {
		sl.Allow(ctx, "user1")
	}
	if res, _ := sl.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at limit: got allowed, want denied")
	}

	// 00:01:00 ちょうど: 最初の5件(00:00:00)がウィンドウ外に出て5枠回復。
	// 00:00:30 の5件はまだウィンドウ内。
	clock.Advance(30 * time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := sl.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("after partial expiry: allowed = %d, want 5", allowed)
	}
}

// TestSlidingWindowLog_NoBoundaryBurst は固定ウィンドウで起きた境界バーストが
// この方式では起きないことを実証する(TestFixedWindow_BoundaryBurst との対比)。
func TestSlidingWindowLog_NoBoundaryBurst(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 10, time.Minute, clock)
	ctx := context.Background()

	// 00:00:59 に10回 → 全部許可(固定ウィンドウと同じ)。
	clock.Advance(59 * time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := sl.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("first burst: allowed = %d, want 10", allowed)
	}

	// 00:01:00: 固定ウィンドウならリセットされてさらに10回通ったが、
	// スライディングウィンドウでは直近60秒に10件あるので全部拒否。
	clock.Advance(time.Second)
	for i := 0; i < 10; i++ {
		if res, _ := sl.Allow(ctx, "user1"); res.Allowed {
			t.Fatal("boundary burst: got allowed, want denied")
		}
	}

	// 00:01:59 を過ぎれば(00:00:59 の記録が期限切れ)再び許可される。
	clock.Advance(59 * time.Second)
	if res, _ := sl.Allow(ctx, "user1"); !res.Allowed {
		t.Error("after expiry: got denied, want allowed")
	}
}

func TestSlidingWindowLog_RetryAfterPointsToOldestExpiry(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 2, time.Minute, clock)
	ctx := context.Background()

	sl.Allow(ctx, "user1") // 00:00:00
	clock.Advance(20 * time.Second)
	sl.Allow(ctx, "user1") // 00:00:20

	clock.Advance(10 * time.Second) // 現在 00:00:30
	res, _ := sl.Allow(ctx, "user1")
	if res.Allowed {
		t.Fatal("got allowed, want denied")
	}
	// 最古の記録(00:00:00)は 00:01:00 に期限切れ → あと30秒。
	if want := 30 * time.Second; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}
}

func TestSlidingWindowLog_KeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 1, time.Minute, clock)
	ctx := context.Background()

	sl.Allow(ctx, "user1")
	if res, _ := sl.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("user1 #2: got allowed, want denied")
	}
	if res, _ := sl.Allow(ctx, "user2"); !res.Allowed {
		t.Error("user2 #1: got denied, want allowed")
	}
}

func TestSlidingWindowLog_ContextCanceled(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 1, time.Minute, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sl.Allow(ctx, "user1"); err == nil {
		t.Error("Allow with canceled context: got nil error, want context.Canceled")
	}
}

func TestSlidingWindowLog_DeleteExpired(t *testing.T) {
	clock := newFakeClock()
	sl := newTestSlidingWindowLog(t, 5, time.Minute, clock)
	ctx := context.Background()

	sl.Allow(ctx, "old")
	clock.Advance(time.Minute) // "old" の全記録が期限切れ
	sl.Allow(ctx, "current")

	sl.deleteExpired(clock.Now())

	sl.mu.Lock()
	defer sl.mu.Unlock()
	if _, ok := sl.logs["old"]; ok {
		t.Error("expired key 'old' was not deleted")
	}
	if _, ok := sl.logs["current"]; !ok {
		t.Error("active key 'current' was deleted")
	}
}

func TestSlidingWindowLog_ConcurrentAccess(t *testing.T) {
	const (
		limit      = 100
		goroutines = 20
		perG       = 10
	)
	sl, err := NewSlidingWindowLog(limit, time.Hour, WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Close()

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
				res, err := sl.Allow(ctx, "shared")
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

func BenchmarkSlidingWindowLog_SingleKey(b *testing.B) {
	// 現実的な limit で、拒否判定(ログ満杯)を含む性能を測る。
	sl, err := NewSlidingWindowLog(1000, time.Hour, WithCleanupInterval(0))
	if err != nil {
		b.Fatal(err)
	}
	defer sl.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sl.Allow(ctx, "bench") //nolint:errcheck
		}
	})
}
