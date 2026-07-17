package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SlidingWindowLog はスライディングウィンドウログ方式のレートリミッター。
//
// key ごとに許可したリクエストのタイムスタンプをすべて記録し、判定のたびに
// 「現在から window だけ遡った範囲」に含まれる件数を数える。ウィンドウが
// 壁時計に固定されず現在時刻とともに滑るため、固定ウィンドウの境界バースト
// 問題が起きず、任意の連続する window 幅で厳密に limit 以下を保証する。
//
// 代償として key あたり最大 limit 個のタイムスタンプ(64-bit 環境で1個24バイト)を
// 保持するため、limit が大きいとメモリを消費する。
// 詳細は docs/03-sliding-window-log.md を参照。
type SlidingWindowLog struct {
	limit  int
	window time.Duration
	clock  Clock

	mu   sync.Mutex
	logs map[string][]time.Time // 各 key の許可済みリクエスト時刻(昇順)

	stop     chan struct{}
	stopOnce sync.Once
}

var _ Limiter = (*SlidingWindowLog)(nil)

// NewSlidingWindowLog は「直近 window の間に limit 回まで」を厳密に保証する
// スライディングウィンドウログリミッターを生成する。
//
// 使い終わったら Close を呼んでジャニターを停止すること。
func NewSlidingWindowLog(limit int, window time.Duration, opts ...Option) (*SlidingWindowLog, error) {
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

	sl := &SlidingWindowLog{
		limit:  limit,
		window: window,
		clock:  cfg.clock,
		logs:   make(map[string][]time.Time),
		stop:   make(chan struct{}),
	}
	if cfg.cleanupInterval > 0 {
		go runJanitor(sl.stop, cfg.cleanupInterval, func() {
			sl.deleteExpired(sl.clock.Now())
		})
	}
	return sl, nil
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (sl *SlidingWindowLog) Allow(ctx context.Context, key string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := sl.clock.Now()
	cutoff := now.Add(-sl.window) // これ以前(含む)の記録はウィンドウ外

	sl.mu.Lock()
	defer sl.mu.Unlock()

	log := sl.logs[key]

	// ウィンドウ外に出た古い記録を先頭から取り除く(ログは昇順なので先頭だけ見ればよい)。
	drop := 0
	for drop < len(log) && !log[drop].After(cutoff) {
		drop++
	}
	log = log[drop:]

	if len(log) >= sl.limit {
		// 最古の記録がウィンドウ外に出た瞬間に1枠空く。
		freeAt := log[0].Add(sl.window)
		sl.logs[key] = log
		return Result{
			Allowed:    false,
			Limit:      sl.limit,
			Remaining:  0,
			RetryAfter: freeAt.Sub(now),
			ResetAt:    freeAt,
		}, nil
	}

	log = append(log, now)
	sl.logs[key] = log
	return Result{
		Allowed:   true,
		Limit:     sl.limit,
		Remaining: sl.limit - len(log),
		ResetAt:   log[0].Add(sl.window), // 最古の記録が消えて枠が回復し始める時刻
	}, nil
}

// Close はバックグラウンドのジャニターを停止する。複数回呼んでも安全。
func (sl *SlidingWindowLog) Close() error {
	sl.stopOnce.Do(func() { close(sl.stop) })
	return nil
}

// deleteExpired は now 時点で全記録がウィンドウ外になった key を削除する。
// Allow 内の刈り込みは「アクセスされた key」しか処理しないため、アクセスが
// 途絶えた key のメモリはここで回収する。
func (sl *SlidingWindowLog) deleteExpired(now time.Time) {
	cutoff := now.Add(-sl.window)
	sl.mu.Lock()
	defer sl.mu.Unlock()
	for key, log := range sl.logs {
		// ログは昇順なので、最新(末尾)が期限切れなら全件期限切れ。
		if len(log) == 0 || !log[len(log)-1].After(cutoff) {
			delete(sl.logs, key)
		}
	}
}
