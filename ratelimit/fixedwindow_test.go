package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestFixedWindow はテスト用のリミッターを生成する。
// ジャニターは判定ロジックと独立してテストするため起動しない。
func newTestFixedWindow(t *testing.T, limit int, window time.Duration, clock Clock) *FixedWindow {
	t.Helper()
	fw, err := NewFixedWindow(limit, window, WithClock(clock), WithCleanupInterval(0))
	if err != nil {
		t.Fatalf("NewFixedWindow: %v", err)
	}
	t.Cleanup(func() { fw.Close() })
	return fw
}

func TestNewFixedWindow_InvalidArgs(t *testing.T) {
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
			if _, err := NewFixedWindow(tt.limit, tt.window); err == nil {
				t.Errorf("NewFixedWindow(%d, %v) = nil error, want error", tt.limit, tt.window)
			}
		})
	}
}

func TestFixedWindow_AllowUpToLimit(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 3, time.Minute, clock)
	ctx := context.Background()

	// 上限までは許可され、Remaining が減っていく。
	for i := 0; i < 3; i++ {
		res, err := fw.Allow(ctx, "user1")
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

	// 上限を超えたら拒否される。
	res, err := fw.Allow(ctx, "user1")
	if err != nil {
		t.Fatalf("Allow #4: %v", err)
	}
	if res.Allowed {
		t.Error("Allow #4: got allowed, want denied")
	}
	if res.Remaining != 0 {
		t.Errorf("Allow #4: Remaining = %d, want 0", res.Remaining)
	}
	if res.RetryAfter != time.Minute {
		t.Errorf("Allow #4: RetryAfter = %v, want %v", res.RetryAfter, time.Minute)
	}
}

func TestFixedWindow_ResetsOnNewWindow(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 2, time.Minute, clock)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if res, _ := fw.Allow(ctx, "user1"); !res.Allowed {
			t.Fatalf("Allow #%d: got denied, want allowed", i+1)
		}
	}
	if res, _ := fw.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("Allow #3: got allowed, want denied")
	}

	// 次のウィンドウに入ればカウントはリセットされる。
	clock.Advance(time.Minute)
	res, err := fw.Allow(ctx, "user1")
	if err != nil {
		t.Fatalf("Allow after window: %v", err)
	}
	if !res.Allowed {
		t.Error("Allow after window: got denied, want allowed")
	}
	if res.Remaining != 1 {
		t.Errorf("Allow after window: Remaining = %d, want 1", res.Remaining)
	}
}

func TestFixedWindow_KeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 1, time.Minute, clock)
	ctx := context.Background()

	if res, _ := fw.Allow(ctx, "user1"); !res.Allowed {
		t.Fatal("user1 #1: got denied, want allowed")
	}
	if res, _ := fw.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("user1 #2: got allowed, want denied")
	}
	// user1 が上限に達していても user2 には影響しない。
	if res, _ := fw.Allow(ctx, "user2"); !res.Allowed {
		t.Error("user2 #1: got denied, want allowed")
	}
}

// TestFixedWindow_BoundaryBurst は固定ウィンドウの弱点である境界バースト問題を
// 実証するテスト。仕様上の欠陥ではなくアルゴリズムの特性なので、挙動が変わったら
// 気づけるように固定化しておく。詳細は docs/02-fixed-window.md を参照。
func TestFixedWindow_BoundaryBurst(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 10, time.Minute, clock)
	ctx := context.Background()

	// ウィンドウ終端ギリギリ(00:00:59)に10回。
	clock.Advance(59 * time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := fw.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}

	// 次のウィンドウ先頭(00:01:00)でさらに10回。
	clock.Advance(time.Second)
	for i := 0; i < 10; i++ {
		if res, _ := fw.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}

	// 「1分あたり10回」の設定なのに、実質1秒間で20回通ってしまう。
	if allowed != 20 {
		t.Errorf("allowed = %d, want 20 (boundary burst is a known property of fixed window)", allowed)
	}
}

func TestFixedWindow_RetryAfterCountsDown(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 1, time.Minute, clock)
	ctx := context.Background()

	fw.Allow(ctx, "user1")

	// ウィンドウ開始15秒後に拒否された場合、残り45秒待てば次のウィンドウ。
	clock.Advance(15 * time.Second)
	res, _ := fw.Allow(ctx, "user1")
	if res.Allowed {
		t.Fatal("got allowed, want denied")
	}
	if want := 45 * time.Second; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}
}

func TestFixedWindow_ContextCanceled(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 1, time.Minute, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fw.Allow(ctx, "user1"); err == nil {
		t.Error("Allow with canceled context: got nil error, want context.Canceled")
	}
}

func TestFixedWindow_DeleteExpired(t *testing.T) {
	clock := newFakeClock()
	fw := newTestFixedWindow(t, 5, time.Minute, clock)
	ctx := context.Background()

	fw.Allow(ctx, "old")
	clock.Advance(time.Minute) // "old" のウィンドウが終了
	fw.Allow(ctx, "current")

	fw.deleteExpired(clock.Now())

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if _, ok := fw.counters["old"]; ok {
		t.Error("expired key 'old' was not deleted")
	}
	if _, ok := fw.counters["current"]; !ok {
		t.Error("active key 'current' was deleted")
	}
}

// TestFixedWindow_ConcurrentAccess は並行アクセス下でも上限を超えて許可
// しないことを検証する。-race フラグ付きで実行することでデータ競合も検出する。
func TestFixedWindow_ConcurrentAccess(t *testing.T) {
	const (
		limit      = 100
		goroutines = 20
		perG       = 10 // 合計 200 リクエスト > limit
	)
	// 実クロックを使うが、window を長く取るのでテスト中に切り替わらない。
	fw, err := NewFixedWindow(limit, time.Hour, WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

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
				res, err := fw.Allow(ctx, "shared")
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

func BenchmarkFixedWindow_SingleKey(b *testing.B) {
	fw, err := NewFixedWindow(1_000_000_000, time.Hour, WithCleanupInterval(0))
	if err != nil {
		b.Fatal(err)
	}
	defer fw.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fw.Allow(ctx, "bench") //nolint:errcheck
		}
	})
}

func BenchmarkFixedWindow_ManyKeys(b *testing.B) {
	fw, err := NewFixedWindow(1_000_000_000, time.Hour, WithCleanupInterval(0))
	if err != nil {
		b.Fatal(err)
	}
	defer fw.Close()
	ctx := context.Background()

	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("user-%d", i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fw.Allow(ctx, keys[i%len(keys)]) //nolint:errcheck
			i++
		}
	})
}
