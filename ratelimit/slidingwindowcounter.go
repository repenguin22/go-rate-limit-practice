package ratelimit

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// SlidingWindowCounter はスライディングウィンドウカウンタ方式のレートリミッター。
//
// スライディングウィンドウログの「厳密だがメモリを食う」問題を、key あたり
// カウンタ2個(前ウィンドウ・現ウィンドウ)だけで近似的に解決する。
// 前ウィンドウのリクエストが期間中に均等に分布していたと仮定し、
//
//	推定カウント = 前ウィンドウ数 × 重なり率 + 現ウィンドウ数
//
// で「滑るウィンドウ」内の件数を推定する。境界バーストを実用上防ぎつつ、
// メモリは固定ウィンドウ並みに小さい。Cloudflare が採用したことで知られる。
// 詳細は docs/04-sliding-window-counter.md を参照。
type SlidingWindowCounter struct {
	limit  int
	window time.Duration
	clock  Clock

	mu     sync.Mutex
	states map[string]*swcState

	stop     chan struct{}
	stopOnce sync.Once
}

// swcState は1つの key の状態。タイムスタンプの列ではなくカウンタ2個で済む。
type swcState struct {
	windowStart time.Time // 現ウィンドウの開始時刻(window 幅に切り捨て済み)
	prev        int       // 前ウィンドウの確定カウント
	curr        int       // 現ウィンドウのカウント
}

var _ Limiter = (*SlidingWindowCounter)(nil)

// NewSlidingWindowCounter は「直近 window の間におよそ limit 回まで」を
// 少ないメモリで近似するスライディングウィンドウカウンタリミッターを生成する。
//
// 使い終わったら Close を呼んでジャニターを停止すること。
func NewSlidingWindowCounter(limit int, window time.Duration, opts ...Option) (*SlidingWindowCounter, error) {
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

	sc := &SlidingWindowCounter{
		limit:  limit,
		window: window,
		clock:  cfg.clock,
		states: make(map[string]*swcState),
		stop:   make(chan struct{}),
	}
	if cfg.cleanupInterval > 0 {
		go runJanitor(sc.stop, cfg.cleanupInterval, func() {
			sc.deleteExpired(sc.clock.Now())
		})
	}
	return sc, nil
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (sc *SlidingWindowCounter) Allow(ctx context.Context, key string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := sc.clock.Now()
	start := now.Truncate(sc.window)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	st, ok := sc.states[key]
	if !ok {
		st = &swcState{windowStart: start}
		sc.states[key] = st
	} else if !st.windowStart.Equal(start) {
		// ウィンドウが切り替わった。1つ先に進んだだけなら現カウントが
		// 「前ウィンドウ」になる。2つ以上先ならどちらも期限切れ。
		if st.windowStart.Equal(start.Add(-sc.window)) {
			st.prev = st.curr
		} else {
			st.prev = 0
		}
		st.curr = 0
		st.windowStart = start
	}

	// 前ウィンドウとの重なり率(1 → 0 へ線形に減衰)。
	elapsed := now.Sub(start)
	weight := 1 - float64(elapsed)/float64(sc.window)
	estimated := float64(st.prev)*weight + float64(st.curr)

	resetAt := start.Add(sc.window)
	if estimated >= float64(sc.limit) {
		return Result{
			Allowed:    false,
			Limit:      sc.limit,
			Remaining:  0,
			RetryAfter: sc.retryAfter(st, elapsed),
			ResetAt:    resetAt,
		}, nil
	}

	st.curr++
	remaining := sc.limit - int(math.Ceil(estimated+1))
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:   true,
		Limit:     sc.limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, nil
}

// retryAfter は拒否された時点から「推定カウントが limit を下回る」までの
// 時間を計算する。前ウィンドウの重みは時間とともに線形に減るため、
//
//	prev*(1 - e/window) + curr < limit
//
// を e について解けばよい。curr がすでに limit に達している場合は現ウィンドウ内
// では回復しないので、次のウィンドウ開始までの時間を返す(近似)。
func (sc *SlidingWindowCounter) retryAfter(st *swcState, elapsed time.Duration) time.Duration {
	if st.prev > 0 && st.curr < sc.limit {
		e := float64(sc.window) * float64(st.prev+st.curr-sc.limit) / float64(st.prev)
		// 境界ちょうどでは estimated == limit で拒否されるため 1ns 余分に待つ。
		retry := time.Duration(math.Ceil(e)) + 1 - elapsed
		if retry < 0 {
			retry = 0
		}
		return retry
	}
	return sc.window - elapsed
}

// Close はバックグラウンドのジャニターを停止する。複数回呼んでも安全。
func (sc *SlidingWindowCounter) Close() error {
	sc.stopOnce.Do(func() { close(sc.stop) })
	return nil
}

// deleteExpired は判定に影響しなくなった key の状態を削除する。
// 現ウィンドウのカウントは次のウィンドウでも prev として参照されるため、
// windowStart から2ウィンドウ経過して初めて安全に消せる。
func (sc *SlidingWindowCounter) deleteExpired(now time.Time) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for key, st := range sc.states {
		if !now.Before(st.windowStart.Add(2 * sc.window)) {
			delete(sc.states, key)
		}
	}
}
