package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FixedWindow は固定ウィンドウカウンタ方式のレートリミッター。
//
// 時間軸を window 幅で区切り(例: 毎分 00 秒〜59 秒)、各ウィンドウ内の
// リクエスト数を key ごとにカウントする。カウントが limit に達したら
// ウィンドウが切り替わるまで拒否する。
//
// 実装が単純で高速・省メモリな一方、ウィンドウ境界の前後に集中した
// リクエストは最大で limit の2倍まで通過しうる(境界バースト問題)。
// 詳細は docs/02-fixed-window.md を参照。
type FixedWindow struct {
	limit  int
	window time.Duration
	clock  Clock

	mu       sync.Mutex
	counters map[string]*windowCounter

	stop     chan struct{}
	stopOnce sync.Once
}

// windowCounter は1つの key の現在ウィンドウの状態。
type windowCounter struct {
	start time.Time // ウィンドウの開始時刻(window 幅に切り捨て済み)
	count int
}

var _ Limiter = (*FixedWindow)(nil)

// NewFixedWindow は「window あたり limit 回まで」を許可する固定ウィンドウ
// リミッターを生成する。
//
// 期限切れになった key の状態は内部のジャニターが定期的に削除する。
// 使い終わったら Close を呼んでジャニターを停止すること。
func NewFixedWindow(limit int, window time.Duration, opts ...Option) (*FixedWindow, error) {
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

	fw := &FixedWindow{
		limit:    limit,
		window:   window,
		clock:    cfg.clock,
		counters: make(map[string]*windowCounter),
		stop:     make(chan struct{}),
	}
	if cfg.cleanupInterval > 0 {
		go runJanitor(fw.stop, cfg.cleanupInterval, func() {
			fw.deleteExpired(fw.clock.Now())
		})
	}
	return fw, nil
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (fw *FixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := fw.clock.Now()
	// ウィンドウは壁時計に整列させる(例: window=1分なら毎分ちょうどに切り替わる)。
	start := now.Truncate(fw.window)
	resetAt := start.Add(fw.window)

	fw.mu.Lock()
	defer fw.mu.Unlock()

	c, ok := fw.counters[key]
	if !ok || !c.start.Equal(start) {
		// 初回アクセス、または前のウィンドウが終わっていたら作り直す。
		c = &windowCounter{start: start}
		fw.counters[key] = c
	}

	if c.count >= fw.limit {
		return Result{
			Allowed:    false,
			Limit:      fw.limit,
			Remaining:  0,
			RetryAfter: resetAt.Sub(now),
			ResetAt:    resetAt,
		}, nil
	}

	c.count++
	return Result{
		Allowed:   true,
		Limit:     fw.limit,
		Remaining: fw.limit - c.count,
		ResetAt:   resetAt,
	}, nil
}

// Close はバックグラウンドのジャニターを停止する。複数回呼んでも安全。
func (fw *FixedWindow) Close() error {
	fw.stopOnce.Do(func() { close(fw.stop) })
	return nil
}

// deleteExpired は now 時点でウィンドウが終了している key の状態を削除する。
// 削除された key は次のアクセス時に新しいウィンドウとして再作成されるため、
// 判定結果には影響しない(純粋なメモリ回収)。
func (fw *FixedWindow) deleteExpired(now time.Time) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	for key, c := range fw.counters {
		if !now.Before(c.start.Add(fw.window)) {
			delete(fw.counters, key)
		}
	}
}
