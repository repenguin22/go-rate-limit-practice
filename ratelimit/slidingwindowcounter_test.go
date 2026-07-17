package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestSlidingWindowCounter(t *testing.T, limit int, window time.Duration, clock Clock) *SlidingWindowCounter {
	t.Helper()
	sc, err := NewSlidingWindowCounter(limit, window, WithClock(clock), WithCleanupInterval(0))
	if err != nil {
		t.Fatalf("NewSlidingWindowCounter: %v", err)
	}
	t.Cleanup(func() { sc.Close() })
	return sc
}

func TestNewSlidingWindowCounter_InvalidArgs(t *testing.T) {
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
			if _, err := NewSlidingWindowCounter(tt.limit, tt.window); err == nil {
				t.Errorf("NewSlidingWindowCounter(%d, %v) = nil error, want error", tt.limit, tt.window)
			}
		})
	}
}

// 前ウィンドウが空なら固定ウィンドウと同じ挙動(estimated = curr)。
func TestSlidingWindowCounter_AllowUpToLimit(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 3, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := sc.Allow(ctx, "user1")
		if err != nil {
			t.Fatalf("Allow #%d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("Allow #%d: got denied, want allowed", i+1)
		}
	}
	if res, _ := sc.Allow(ctx, "user1"); res.Allowed {
		t.Error("Allow #4: got allowed, want denied")
	}
}

// TestSlidingWindowCounter_WeightedEstimate は近似計算の核心を検証する。
// 前ウィンドウで上限いっぱい使った場合、次ウィンドウの中間点では
// 「前カウント×0.5」が持ち越されるため、残り半分しか許可されない。
func TestSlidingWindowCounter_WeightedEstimate(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 10, time.Minute, clock)
	ctx := context.Background()

	// ウィンドウ1(00:00:00〜)で10回使い切る。
	for i := 0; i < 10; i++ {
		if res, _ := sc.Allow(ctx, "user1"); !res.Allowed {
			t.Fatalf("window1 #%d: got denied, want allowed", i+1)
		}
	}

	// ウィンドウ2の中間点(00:01:30)。重み = 0.5 なので
	// estimated = 10×0.5 + curr。curr が5になるまで許可される。
	clock.Advance(90 * time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := sc.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("at window midpoint: allowed = %d, want 5", allowed)
	}
}

// TestSlidingWindowCounter_MitigatesBoundaryBurst は固定ウィンドウの
// 境界バースト(1秒間に2倍通る)がこの方式では防がれることを実証する。
func TestSlidingWindowCounter_MitigatesBoundaryBurst(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 10, time.Minute, clock)
	ctx := context.Background()

	// 00:00:59 に10回 → 許可される。
	clock.Advance(59 * time.Second)
	for i := 0; i < 10; i++ {
		if res, _ := sc.Allow(ctx, "user1"); !res.Allowed {
			t.Fatalf("first burst #%d: got denied, want allowed", i+1)
		}
	}

	// 00:01:00(次ウィンドウ先頭): 重み = 1.0 なので estimated = 10 → 全拒否。
	// 固定ウィンドウではここでさらに10回通っていた。
	clock.Advance(time.Second)
	for i := 0; i < 10; i++ {
		if res, _ := sc.Allow(ctx, "user1"); res.Allowed {
			t.Fatal("boundary: got allowed, want denied")
		}
	}
}

// 重みの減衰に合わせて枠が徐々に回復することを検証する。
func TestSlidingWindowCounter_GradualRecovery(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		sc.Allow(ctx, "user1")
	}

	// 00:01:03(重み 0.95): estimated = 9.5 < 10 → 1回許可される。
	clock.Advance(63 * time.Second)
	if res, _ := sc.Allow(ctx, "user1"); !res.Allowed {
		t.Fatal("at weight 0.95: got denied, want allowed")
	}
	// estimated = 9.5 + 1 = 10.5 → 拒否。
	res, _ := sc.Allow(ctx, "user1")
	if res.Allowed {
		t.Fatal("at weight 0.95 #2: got allowed, want denied")
	}
	// 10×(1−e/60)+1 < 10 となるのは e > 6s。現在 e=3s なので約3秒待ち。
	if res.RetryAfter < 3*time.Second || res.RetryAfter > 3*time.Second+time.Millisecond {
		t.Errorf("RetryAfter = %v, want ~3s", res.RetryAfter)
	}

	clock.Advance(res.RetryAfter)
	if res, _ := sc.Allow(ctx, "user1"); !res.Allowed {
		t.Error("after RetryAfter: got denied, want allowed")
	}
}

// 2ウィンドウ以上空くと状態が完全にリセットされることを検証する。
func TestSlidingWindowCounter_FullResetAfterGap(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 5, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		sc.Allow(ctx, "user1")
	}
	clock.Advance(2 * time.Minute)

	allowed := 0
	for i := 0; i < 5; i++ {
		if res, _ := sc.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("after 2-window gap: allowed = %d, want 5", allowed)
	}
}

func TestSlidingWindowCounter_KeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 1, time.Minute, clock)
	ctx := context.Background()

	sc.Allow(ctx, "user1")
	if res, _ := sc.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("user1 #2: got allowed, want denied")
	}
	if res, _ := sc.Allow(ctx, "user2"); !res.Allowed {
		t.Error("user2 #1: got denied, want allowed")
	}
}

func TestSlidingWindowCounter_ContextCanceled(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 1, time.Minute, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sc.Allow(ctx, "user1"); err == nil {
		t.Error("Allow with canceled context: got nil error, want context.Canceled")
	}
}

func TestSlidingWindowCounter_DeleteExpired(t *testing.T) {
	clock := newFakeClock()
	sc := newTestSlidingWindowCounter(t, 5, time.Minute, clock)
	ctx := context.Background()

	sc.Allow(ctx, "old")
	clock.Advance(time.Minute)
	sc.Allow(ctx, "recent") // recent はまだ prev として意味を持つ期間

	// old は2ウィンドウ経過で削除対象、recent は1ウィンドウなので残る。
	clock.Advance(time.Minute)
	sc.deleteExpired(clock.Now())

	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.states["old"]; ok {
		t.Error("expired key 'old' was not deleted")
	}
	if _, ok := sc.states["recent"]; !ok {
		t.Error("key 'recent' was deleted while still relevant")
	}
}

func TestSlidingWindowCounter_ConcurrentAccess(t *testing.T) {
	const (
		limit      = 100
		goroutines = 20
		perG       = 10
	)
	sc, err := NewSlidingWindowCounter(limit, time.Hour, WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

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
				res, err := sc.Allow(ctx, "shared")
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

func BenchmarkSlidingWindowCounter_SingleKey(b *testing.B) {
	sc, err := NewSlidingWindowCounter(1_000_000_000, time.Hour, WithCleanupInterval(0))
	if err != nil {
		b.Fatal(err)
	}
	defer sc.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sc.Allow(ctx, "bench") //nolint:errcheck
		}
	})
}
