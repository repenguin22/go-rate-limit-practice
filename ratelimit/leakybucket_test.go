package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestLeakyBucket(t *testing.T, limit int, window time.Duration, clock Clock, opts ...Option) *LeakyBucket {
	t.Helper()
	opts = append([]Option{WithClock(clock), WithCleanupInterval(0)}, opts...)
	lb, err := NewLeakyBucket(limit, window, opts...)
	if err != nil {
		t.Fatalf("NewLeakyBucket: %v", err)
	}
	t.Cleanup(func() { lb.Close() })
	return lb
}

func TestNewLeakyBucket_InvalidArgs(t *testing.T) {
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
		{"negative capacity", 3, time.Second, []Option{WithBurst(-1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewLeakyBucket(tt.limit, tt.window, tt.opts...); err == nil {
				t.Errorf("NewLeakyBucket(%d, %v) = nil error, want error", tt.limit, tt.window)
			}
		})
	}
}

// 空のバケツには容量いっぱいまで注げる。
func TestLeakyBucket_FillToCapacity(t *testing.T) {
	clock := newFakeClock()
	lb := newTestLeakyBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		res, err := lb.Allow(ctx, "user1")
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
	if res, _ := lb.Allow(ctx, "user1"); res.Allowed {
		t.Error("Allow #11: got allowed, want denied (bucket full)")
	}
}

// 水は一定速度で漏れ、漏れた分だけ注げるようになる。
func TestLeakyBucket_SteadyLeak(t *testing.T) {
	clock := newFakeClock()
	// 10単位/分 = 6秒に1単位漏れる。
	lb := newTestLeakyBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		lb.Allow(ctx, "user1")
	}

	// 3秒後: まだ0.5単位しか漏れていない → 拒否。
	clock.Advance(3 * time.Second)
	if res, _ := lb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at +3s: got allowed, want denied")
	}

	// 6秒後: 1単位漏れた → 1回だけ許可。
	clock.Advance(3 * time.Second)
	if res, _ := lb.Allow(ctx, "user1"); !res.Allowed {
		t.Fatal("at +6s: got denied, want allowed")
	}
	if res, _ := lb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at +6s #2: got allowed, want denied")
	}
}

// TestLeakyBucket_CapacityOneSmoothing は容量1の完全平滑化を検証する。
// 「最低6秒間隔でしかリクエストできない」ようになり、バーストが一切許されない。
// これがトークンバケットとの実用上の使い分けポイント。
func TestLeakyBucket_CapacityOneSmoothing(t *testing.T) {
	clock := newFakeClock()
	// 10回/分・容量1 = 最低6秒間隔を強制。
	lb := newTestLeakyBucket(t, 10, time.Minute, clock, WithBurst(1))
	ctx := context.Background()

	if res, _ := lb.Allow(ctx, "user1"); !res.Allowed {
		t.Fatal("#1: got denied, want allowed")
	}
	// 直後の2回目はバーストとして通らない。
	if res, _ := lb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("#2 immediately: got allowed, want denied")
	}

	// 6秒待てば1回通る。
	clock.Advance(6 * time.Second)
	if res, _ := lb.Allow(ctx, "user1"); !res.Allowed {
		t.Error("after 6s: got denied, want allowed")
	}
}

func TestLeakyBucket_RetryAfter(t *testing.T) {
	clock := newFakeClock()
	lb := newTestLeakyBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		lb.Allow(ctx, "user1")
	}

	// 満水(水位10) → 水位9になるまで6秒。
	res, _ := lb.Allow(ctx, "user1")
	if res.Allowed {
		t.Fatal("got allowed, want denied")
	}
	if want := 6 * time.Second; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}

	// RetryAfter どおりに待てば必ず通る。
	clock.Advance(res.RetryAfter)
	if res, _ := lb.Allow(ctx, "user1"); !res.Allowed {
		t.Error("after waiting RetryAfter: got denied, want allowed")
	}
}

// 長時間放置すればバケツは空に戻る(それ以上は空かない)。
func TestLeakyBucket_DrainsToEmpty(t *testing.T) {
	clock := newFakeClock()
	lb := newTestLeakyBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		lb.Allow(ctx, "user1")
	}
	clock.Advance(24 * time.Hour)

	allowed := 0
	for i := 0; i < 100; i++ {
		if res, _ := lb.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("after 24h idle: allowed = %d, want 10 (capacity)", allowed)
	}
}

func TestLeakyBucket_KeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	lb := newTestLeakyBucket(t, 1, time.Minute, clock)
	ctx := context.Background()

	lb.Allow(ctx, "user1")
	if res, _ := lb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("user1 #2: got allowed, want denied")
	}
	if res, _ := lb.Allow(ctx, "user2"); !res.Allowed {
		t.Error("user2 #1: got denied, want allowed")
	}
}

func TestLeakyBucket_ContextCanceled(t *testing.T) {
	clock := newFakeClock()
	lb := newTestLeakyBucket(t, 1, time.Minute, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lb.Allow(ctx, "user1"); err == nil {
		t.Error("Allow with canceled context: got nil error, want context.Canceled")
	}
}

func TestLeakyBucket_DeleteExpired(t *testing.T) {
	clock := newFakeClock()
	lb := newTestLeakyBucket(t, 10, time.Minute, clock)
	ctx := context.Background()

	lb.Allow(ctx, "idle") // 水位1。空になるまで6秒
	clock.Advance(6 * time.Second)
	lb.Allow(ctx, "active") // 水位1。まだ空でない

	lb.deleteExpired(clock.Now())

	lb.mu.Lock()
	defer lb.mu.Unlock()
	if _, ok := lb.states["idle"]; ok {
		t.Error("drained key 'idle' was not deleted")
	}
	if _, ok := lb.states["active"]; !ok {
		t.Error("non-empty key 'active' was deleted")
	}
}

func TestLeakyBucket_ConcurrentAccess(t *testing.T) {
	const (
		limit      = 100
		goroutines = 20
		perG       = 10
	)
	// 漏れがテスト中に起きないよう、レートは極端に遅くする。
	lb, err := NewLeakyBucket(limit, 24*time.Hour, WithBurst(limit), WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer lb.Close()

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
				res, err := lb.Allow(ctx, "shared")
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

func BenchmarkLeakyBucket_SingleKey(b *testing.B) {
	lb, err := NewLeakyBucket(1_000_000_000, time.Second, WithCleanupInterval(0))
	if err != nil {
		b.Fatal(err)
	}
	defer lb.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lb.Allow(ctx, "bench") //nolint:errcheck
		}
	})
}
