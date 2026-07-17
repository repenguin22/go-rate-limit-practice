package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

// recordingLimiter は受け取った key を記録し、固定の結果を返すテスト用リミッター。
type recordingLimiter struct {
	keys   []string
	res    Result
	err    error
	closed int
}

func (r *recordingLimiter) Allow(_ context.Context, key string) (Result, error) {
	r.keys = append(r.keys, key)
	return r.res, r.err
}

func (r *recordingLimiter) Close() error {
	r.closed++
	return nil
}

func TestNewTierLimiter_InvalidArgs(t *testing.T) {
	ok := &recordingLimiter{}

	if _, err := NewTierLimiter(map[string]Limiter{"free": ok}, nil); err == nil {
		t.Error("nil fallback: got nil error, want error")
	}
	if _, err := NewTierLimiter(map[string]Limiter{"free": nil}, ok); err == nil {
		t.Error("nil tier limiter: got nil error, want error")
	}
	// limiters が空(すべて fallback 行き)は正当な構成。
	if _, err := NewTierLimiter(nil, ok); err != nil {
		t.Errorf("nil limiters map: got error %v, want nil", err)
	}
}

// tier ごとに異なる制限が適用されることを本物のリミッターで検証する。
func TestTierLimiter_RoutesByTier(t *testing.T) {
	clock := newFakeClock()
	free := newTestFixedWindow(t, 1, time.Minute, clock) // 1回/分
	pro := newTestFixedWindow(t, 3, time.Minute, clock)  // 3回/分
	tl, err := NewTierLimiter(map[string]Limiter{"free": free, "pro": pro}, free)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// free の alice は1回で上限。
	if res, _ := tl.Allow(ctx, TierKey("free", "alice")); !res.Allowed {
		t.Fatal("free alice #1: got denied, want allowed")
	}
	if res, _ := tl.Allow(ctx, TierKey("free", "alice")); res.Allowed {
		t.Error("free alice #2: got allowed, want denied")
	}

	// pro の bob は3回まで通る。Result.Limit も pro の値になる。
	for i := 0; i < 3; i++ {
		res, _ := tl.Allow(ctx, TierKey("pro", "bob"))
		if !res.Allowed {
			t.Fatalf("pro bob #%d: got denied, want allowed", i+1)
		}
		if res.Limit != 3 {
			t.Errorf("pro bob #%d: Limit = %d, want 3", i+1, res.Limit)
		}
	}
	if res, _ := tl.Allow(ctx, TierKey("pro", "bob")); res.Allowed {
		t.Error("pro bob #4: got allowed, want denied")
	}
}

// 同じクライアントIDでも tier が違えば状態は独立している。
func TestTierLimiter_SameClientDifferentTiersAreIndependent(t *testing.T) {
	clock := newFakeClock()
	free := newTestFixedWindow(t, 1, time.Minute, clock)
	pro := newTestFixedWindow(t, 1, time.Minute, clock)
	tl, err := NewTierLimiter(map[string]Limiter{"free": free, "pro": pro}, free)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	tl.Allow(ctx, TierKey("free", "alice"))
	if res, _ := tl.Allow(ctx, TierKey("free", "alice")); res.Allowed {
		t.Fatal("free alice: got allowed, want denied")
	}
	// pro 側の alice は別リミッターなので影響を受けない。
	if res, _ := tl.Allow(ctx, TierKey("pro", "alice")); !res.Allowed {
		t.Error("pro alice: got denied, want allowed")
	}
}

// tier リミッターには「クライアント部だけ」が key として渡る。
func TestTierLimiter_StripsTierPrefixForKnownTier(t *testing.T) {
	free := &recordingLimiter{res: Result{Allowed: true}}
	tl, err := NewTierLimiter(map[string]Limiter{"free": free}, &recordingLimiter{})
	if err != nil {
		t.Fatal(err)
	}

	tl.Allow(context.Background(), "free:alice")
	if len(free.keys) != 1 || free.keys[0] != "alice" {
		t.Errorf("tier limiter got keys %v, want [alice]", free.keys)
	}
}

// 未知の tier と区切りなしの key は、元の key 全体のまま fallback に渡る。
// key を丸ごと保つことで、異なる未知 tier の同名クライアントが
// fallback 内で状態を共有しない。
func TestTierLimiter_UnknownTierFallsBackWithFullKey(t *testing.T) {
	fallback := &recordingLimiter{res: Result{Allowed: true}}
	tl, err := NewTierLimiter(map[string]Limiter{"free": &recordingLimiter{}}, fallback)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	tl.Allow(ctx, "gold:alice")   // 未知 tier
	tl.Allow(ctx, "silver:alice") // 別の未知 tier
	tl.Allow(ctx, "no-separator") // 区切りなし

	want := []string{"gold:alice", "silver:alice", "no-separator"}
	if len(fallback.keys) != len(want) {
		t.Fatalf("fallback got %d keys %v, want %v", len(fallback.keys), fallback.keys, want)
	}
	for i, k := range want {
		if fallback.keys[i] != k {
			t.Errorf("fallback key[%d] = %q, want %q", i, fallback.keys[i], k)
		}
	}
}

// 委譲先のエラー(Redis障害など)はそのまま呼び出し元へ伝わる。
func TestTierLimiter_PropagatesError(t *testing.T) {
	wantErr := errors.New("redis: connection refused")
	free := &recordingLimiter{err: wantErr}
	tl, err := NewTierLimiter(map[string]Limiter{"free": free}, &recordingLimiter{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tl.Allow(context.Background(), "free:alice"); !errors.Is(err, wantErr) {
		t.Errorf("got error %v, want %v", err, wantErr)
	}
}

// Close は全リミッターを閉じる。同一インスタンスの重複登録は一度だけ。
func TestTierLimiter_CloseClosesEachLimiterOnce(t *testing.T) {
	shared := &recordingLimiter{} // "free" と "trial" と fallback を兼ねる
	pro := &recordingLimiter{}
	tl, err := NewTierLimiter(map[string]Limiter{
		"free":  shared,
		"trial": shared,
		"pro":   pro,
	}, shared)
	if err != nil {
		t.Fatal(err)
	}

	if err := tl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if shared.closed != 1 {
		t.Errorf("shared limiter closed %d times, want 1", shared.closed)
	}
	if pro.closed != 1 {
		t.Errorf("pro limiter closed %d times, want 1", pro.closed)
	}
}
