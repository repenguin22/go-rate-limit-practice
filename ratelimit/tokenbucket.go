package ratelimit

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucket はトークンバケット方式のレートリミッター。
//
// key ごとに容量 burst のバケツを持ち、window あたり limit 個の一定速度で
// トークンが補充される。リクエストはトークンを1個消費して通過し、
// トークンがなければ拒否される。
//
// 「平均レートは limit/window に抑えつつ、バケツに貯まった分までの
// 瞬間バーストは許容する」という特性が実務の要求と合致しやすく、
// 最も広く使われている方式。golang.org/x/time/rate もこの方式。
// 詳細は docs/05-token-bucket.md を参照。
type TokenBucket struct {
	limit  int           // window あたりの補充数(平均レートの分子)
	window time.Duration // 平均レートの分母
	burst  float64       // バケツの容量
	clock  Clock

	mu     sync.Mutex
	states map[string]*tbState

	stop     chan struct{}
	stopOnce sync.Once
}

// tbState は1つの key のバケツの状態。
// トークン残量は「最後に見た時刻」と組で持ち、アクセス時に経過時間分を
// まとめて補充する(遅延補充)。バックグラウンドで補充し続ける必要はない。
type tbState struct {
	tokens float64   // 現在のトークン残量(小数になりうる)
	last   time.Time // 最後に補充計算をした時刻
}

var _ Limiter = (*TokenBucket)(nil)

// NewTokenBucket は「平均 window あたり limit 回、瞬間バーストは burst 回まで」
// を許可するトークンバケットリミッターを生成する。
// burst は WithBurst で指定し、未指定なら limit と同じ。
//
// 使い終わったら Close を呼んでジャニターを停止すること。
func NewTokenBucket(limit int, window time.Duration, opts ...Option) (*TokenBucket, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("ratelimit: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("ratelimit: window must be positive, got %v", window)
	}

	cfg := defaultConfig(window)
	for _, opt := range opts {
		opt(&cfg)
	}
	burst := cfg.burst
	if burst == 0 {
		burst = limit
	}
	if burst < 0 {
		return nil, fmt.Errorf("ratelimit: burst must be positive, got %d", burst)
	}

	tb := &TokenBucket{
		limit:  limit,
		window: window,
		burst:  float64(burst),
		clock:  cfg.clock,
		states: make(map[string]*tbState),
		stop:   make(chan struct{}),
	}
	if cfg.cleanupInterval > 0 {
		go runJanitor(tb.stop, cfg.cleanupInterval, func() {
			tb.deleteExpired(tb.clock.Now())
		})
	}
	return tb, nil
}

// tokensIn は経過時間 d の間に補充されるトークン数。
// 除算を最後に行うことで浮動小数点誤差を最小にする
// (rate = limit/window を先に計算すると 6e9 × (10/6e10) ≠ 1.0 のような誤差が出る)。
func (tb *TokenBucket) tokensIn(d time.Duration) float64 {
	return float64(d) * float64(tb.limit) / float64(tb.window)
}

// durationFor はトークンが n 個補充されるのにかかる時間(ナノ秒切り上げ)。
func (tb *TokenBucket) durationFor(n float64) time.Duration {
	return time.Duration(math.Ceil(n * float64(tb.window) / float64(tb.limit)))
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (tb *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := tb.clock.Now()

	tb.mu.Lock()
	defer tb.mu.Unlock()

	st, ok := tb.states[key]
	if !ok {
		// 新しい key はバケツ満タンから始まる(=いきなり burst 回使える)。
		st = &tbState{tokens: tb.burst, last: now}
		tb.states[key] = st
	} else {
		// 前回からの経過時間分をまとめて補充する。容量は超えない。
		st.tokens = math.Min(tb.burst, st.tokens+tb.tokensIn(now.Sub(st.last)))
		st.last = now
	}

	if st.tokens < 1 {
		// 残り (1 - tokens) 個が貯まるまでの時間が待ち時間。
		retry := tb.durationFor(1 - st.tokens)
		return Result{
			Allowed:    false,
			Limit:      int(tb.burst),
			Remaining:  0,
			RetryAfter: retry,
			ResetAt:    now.Add(tb.timeToFull(st.tokens)),
		}, nil
	}

	st.tokens--
	return Result{
		Allowed:   true,
		Limit:     int(tb.burst),
		Remaining: int(st.tokens),
		ResetAt:   now.Add(tb.timeToFull(st.tokens)),
	}, nil
}

// timeToFull は現在の残量からバケツが満タンに戻るまでの時間。
func (tb *TokenBucket) timeToFull(tokens float64) time.Duration {
	if tokens >= tb.burst {
		return 0
	}
	return tb.durationFor(tb.burst - tokens)
}

// Close はバックグラウンドのジャニターを停止する。複数回呼んでも安全。
func (tb *TokenBucket) Close() error {
	tb.stopOnce.Do(func() { close(tb.stop) })
	return nil
}

// deleteExpired はバケツが満タンに戻っている key を削除する。
// 満タンの状態は新規 key と区別がつかないため、消しても判定に影響しない。
func (tb *TokenBucket) deleteExpired(now time.Time) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	for key, st := range tb.states {
		if st.tokens+tb.tokensIn(now.Sub(st.last)) >= tb.burst {
			delete(tb.states, key)
		}
	}
}
