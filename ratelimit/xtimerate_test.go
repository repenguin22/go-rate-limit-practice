package ratelimit

// このファイルは Stage 7(x/time/rate との比較)のためのテスト。
// 自作 TokenBucket と golang.org/x/time/rate が同じアルゴリズムであることを
// 「同じ入力列に対して同じ判定を返す」ことで実証する。
// golang.org/x/time はこの比較のためだけの依存で、実装からは参照しない。

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestTokenBucket_MatchesXTimeRate は同一パラメータ・同一のリクエスト時系列を
// 両実装に与え、全リクエストで許可/拒否が一致することを検証する。
//
// x/time/rate はクロック注入の代わりに「時刻を引数で渡す」方式
// (AllowN(t, n))を採用しているため、こちらには偽クロックが要らない。
// テスト容易性への2つのアプローチが並ぶ点も見どころ。
func TestTokenBucket_MatchesXTimeRate(t *testing.T) {
	clock := newFakeClock()
	// 10個/分 = 6秒に1個。バースト10。
	tb := newTestTokenBucket(t, 10, time.Minute, clock)
	xl := rate.NewLimiter(rate.Every(6*time.Second), 10)
	ctx := context.Background()

	steps := []struct {
		name    string
		advance time.Duration
		n       int // この時点で連続して送るリクエスト数
	}{
		{"initial burst", 0, 12},             // 10許可 + 2拒否のはず
		{"half refill", 3 * time.Second, 1},  // 0.5個 → 拒否
		{"one refill", 3 * time.Second, 2},   // 1個 → 1許可 + 1拒否
		{"full refill", 2 * time.Minute, 12}, // 満タン → 10許可 + 2拒否
		{"steady pace", 6 * time.Second, 1},  // ちょうど1個 → 許可
	}
	for _, step := range steps {
		clock.Advance(step.advance)
		now := clock.Now()
		for i := 0; i < step.n; i++ {
			res, err := tb.Allow(ctx, "k")
			if err != nil {
				t.Fatalf("%s #%d: %v", step.name, i+1, err)
			}
			want := xl.AllowN(now, 1)
			if res.Allowed != want {
				t.Errorf("%s #%d: TokenBucket=%v, x/time/rate=%v",
					step.name, i+1, res.Allowed, want)
			}
		}
	}
}

// 自作実装(BenchmarkTokenBucket_SingleKey)との性能比較用。
// x/time/rate は key を持たない単一リミッターなので、その分の
// map参照コストがない点を踏まえて読むこと。
func BenchmarkXTimeRate_Single(b *testing.B) {
	xl := rate.NewLimiter(rate.Limit(1_000_000_000), 1_000_000_000)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			xl.Allow()
		}
	})
}
