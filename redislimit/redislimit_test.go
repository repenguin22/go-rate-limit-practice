package redislimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// baseTime はテストの基準時刻。miniredis の SetTime で Redis サーバー時刻を
// 固定・進行させることで、TIME を使うスクリプトを決定的にテストする
// (インメモリ版の fakeClock に相当する役割を miniredis が担う)。
var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// setup はテスト用の miniredis とクライアントを起動する。
func setup(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(baseTime)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return mr, client
}

// brokenClient は接続先のない(必ず失敗する)クライアントを返す。
func brokenClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr:            "127.0.0.1:1", // 到達不能
		DialTimeout:     50 * time.Millisecond,
		MaxRetries:      -1,
		PoolTimeout:     50 * time.Millisecond,
		MinIdleConns:    0,
		ConnMaxIdleTime: -1,
	})
	t.Cleanup(func() { client.Close() })
	return client
}

func TestValidate_InvalidArgs(t *testing.T) {
	_, client := setup(t)

	if _, err := NewFixedWindow(nil, 3, time.Second); err == nil {
		t.Error("nil client: got nil error, want error")
	}
	if _, err := NewFixedWindow(client, 0, time.Second); err == nil {
		t.Error("zero limit: got nil error, want error")
	}
	if _, err := NewSlidingWindow(client, 3, 0); err == nil {
		t.Error("zero window: got nil error, want error")
	}
	if _, err := NewTokenBucket(client, 3, time.Second, WithBurst(-1)); err == nil {
		t.Error("negative burst: got nil error, want error")
	}
}

// --- FixedWindow ---

func TestFixedWindow_AllowUpToLimit(t *testing.T) {
	_, client := setup(t)
	fw, err := NewFixedWindow(client, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

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

	res, err := fw.Allow(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Error("Allow #4: got allowed, want denied")
	}
	if res.RetryAfter <= 0 || res.RetryAfter > time.Minute {
		t.Errorf("RetryAfter = %v, want (0, 1m]", res.RetryAfter)
	}
}

// TTL が切れればカウントはリセットされる(掃除もリセットも Redis 任せ)。
func TestFixedWindow_ResetsAfterTTL(t *testing.T) {
	mr, client := setup(t)
	fw, err := NewFixedWindow(client, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	fw.Allow(ctx, "user1")
	fw.Allow(ctx, "user1")
	if res, _ := fw.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at limit: got allowed, want denied")
	}

	mr.FastForward(time.Minute) // TTL を経過させる
	res, err := fw.Allow(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Allowed {
		t.Error("after TTL: got denied, want allowed")
	}
}

// TestFixedWindow_SharedAcrossInstances が分散版の存在意義そのもの:
// 別々のリミッターインスタンス(=別々のアプリサーバー)が同じ Redis を
// 参照すれば、制限は合算で効く。
func TestFixedWindow_SharedAcrossInstances(t *testing.T) {
	_, client := setup(t)
	server1, err := NewFixedWindow(client, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	server2, err := NewFixedWindow(client, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// server1 で2回、server2 で1回 → 合計3回で上限。
	server1.Allow(ctx, "user1")
	server1.Allow(ctx, "user1")
	server2.Allow(ctx, "user1")

	// どちらのインスタンスから見ても上限超過。
	if res, _ := server1.Allow(ctx, "user1"); res.Allowed {
		t.Error("server1: got allowed, want denied (limit shared)")
	}
	if res, _ := server2.Allow(ctx, "user1"); res.Allowed {
		t.Error("server2: got allowed, want denied (limit shared)")
	}
}

func TestFixedWindow_KeysAreIndependent(t *testing.T) {
	_, client := setup(t)
	fw, err := NewFixedWindow(client, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	fw.Allow(ctx, "user1")
	if res, _ := fw.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("user1 #2: got allowed, want denied")
	}
	if res, _ := fw.Allow(ctx, "user2"); !res.Allowed {
		t.Error("user2 #1: got denied, want allowed")
	}
}

func TestFixedWindow_RedisDownReturnsError(t *testing.T) {
	fw, err := NewFixedWindow(brokenClient(t), 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Allow(context.Background(), "user1"); err == nil {
		t.Error("redis down: got nil error, want error")
	}
}

// --- SlidingWindow ---

func TestSlidingWindow_AllowUpToLimit(t *testing.T) {
	_, client := setup(t)
	sw, err := NewSlidingWindow(client, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := sw.Allow(ctx, "user1")
		if err != nil {
			t.Fatalf("Allow #%d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("Allow #%d: got denied, want allowed", i+1)
		}
	}
	if res, _ := sw.Allow(ctx, "user1"); res.Allowed {
		t.Error("Allow #4: got allowed, want denied")
	}
}

// インメモリ版 TestSlidingWindowCounter_WeightedEstimate と同じシナリオ。
// 前ウィンドウ満杯 → 次ウィンドウ中間点では半分だけ許可される。
func TestSlidingWindow_WeightedEstimate(t *testing.T) {
	mr, client := setup(t)
	sw, err := NewSlidingWindow(client, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if res, _ := sw.Allow(ctx, "user1"); !res.Allowed {
			t.Fatalf("window1 #%d: got denied, want allowed", i+1)
		}
	}

	// 次ウィンドウの中間点(重み 0.5)へ。TTL も進める必要があるので
	// SetTime と FastForward を両方使う。
	mr.SetTime(baseTime.Add(90 * time.Second))
	mr.FastForward(90 * time.Second)

	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := sw.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("at window midpoint: allowed = %d, want 5", allowed)
	}
}

// インメモリ版と同じ境界バーストシナリオが Redis 版でも防がれる。
func TestSlidingWindow_MitigatesBoundaryBurst(t *testing.T) {
	mr, client := setup(t)
	sw, err := NewSlidingWindow(client, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	mr.SetTime(baseTime.Add(59 * time.Second))
	for i := 0; i < 10; i++ {
		if res, _ := sw.Allow(ctx, "user1"); !res.Allowed {
			t.Fatalf("first burst #%d: got denied, want allowed", i+1)
		}
	}

	mr.SetTime(baseTime.Add(60 * time.Second))
	for i := 0; i < 10; i++ {
		if res, _ := sw.Allow(ctx, "user1"); res.Allowed {
			t.Fatal("boundary: got allowed, want denied")
		}
	}
}

func TestSlidingWindow_SharedAcrossInstances(t *testing.T) {
	_, client := setup(t)
	server1, _ := NewSlidingWindow(client, 2, time.Minute)
	server2, _ := NewSlidingWindow(client, 2, time.Minute)
	ctx := context.Background()

	server1.Allow(ctx, "user1")
	server2.Allow(ctx, "user1")
	if res, _ := server1.Allow(ctx, "user1"); res.Allowed {
		t.Error("got allowed, want denied (limit shared)")
	}
}

// --- TokenBucket ---

func TestTokenBucket_InitialBurstThenDeny(t *testing.T) {
	_, client := setup(t)
	tb, err := NewTokenBucket(client, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		res, err := tb.Allow(ctx, "user1")
		if err != nil {
			t.Fatalf("Allow #%d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("Allow #%d: got denied, want allowed", i+1)
		}
	}

	res, err := tb.Allow(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("Allow #11: got allowed, want denied")
	}
	// トークン0 → 次の1個まで 6秒(= 60s/10)。
	if want := 6 * time.Second; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}
}

// インメモリ版 TestTokenBucket_SteadyRefill と同じシナリオ。
func TestTokenBucket_SteadyRefill(t *testing.T) {
	mr, client := setup(t)
	tb, err := NewTokenBucket(client, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		tb.Allow(ctx, "user1")
	}

	// 3秒後: 0.5個 → 拒否。
	mr.SetTime(baseTime.Add(3 * time.Second))
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at +3s: got allowed, want denied")
	}

	// 6秒後: 1個 → 1回だけ許可。
	mr.SetTime(baseTime.Add(6 * time.Second))
	if res, _ := tb.Allow(ctx, "user1"); !res.Allowed {
		t.Fatal("at +6s: got denied, want allowed")
	}
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Fatal("at +6s #2: got allowed, want denied")
	}
}

func TestTokenBucket_CustomBurst(t *testing.T) {
	_, client := setup(t)
	tb, err := NewTokenBucket(client, 1, time.Second, WithBurst(5))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 10; i++ {
		if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("initial burst: allowed = %d, want 5", allowed)
	}
}

// WithBurst(0) は「未指定」としてインメモリ版と同じく limit にフォールバックする。
func TestTokenBucket_BurstZeroMeansLimit(t *testing.T) {
	_, client := setup(t)
	tb, err := NewTokenBucket(client, 2, time.Minute, WithBurst(0))
	if err != nil {
		t.Fatalf("WithBurst(0) should mean unset, got error: %v", err)
	}
	ctx := context.Background()

	tb.Allow(ctx, "user1")
	tb.Allow(ctx, "user1")
	if res, _ := tb.Allow(ctx, "user1"); res.Allowed {
		t.Error("burst should default to limit=2: #3 got allowed, want denied")
	}
}

func TestTokenBucket_SharedAcrossInstances(t *testing.T) {
	_, client := setup(t)
	server1, _ := NewTokenBucket(client, 2, time.Minute)
	server2, _ := NewTokenBucket(client, 2, time.Minute)
	ctx := context.Background()

	server1.Allow(ctx, "user1")
	server2.Allow(ctx, "user1")
	if res, _ := server1.Allow(ctx, "user1"); res.Allowed {
		t.Error("got allowed, want denied (bucket shared)")
	}
}

func TestTokenBucket_RedisDownReturnsError(t *testing.T) {
	tb, err := NewTokenBucket(brokenClient(t), 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tb.Allow(context.Background(), "user1"); err == nil {
		t.Error("redis down: got nil error, want error")
	}
}
