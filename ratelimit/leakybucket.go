package ratelimit

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// LeakyBucket はリーキーバケット方式(メーター型)のレートリミッター。
//
// key ごとに容量 capacity のバケツを持ち、リクエストのたびに水が1単位注がれる。
// 水は window あたり limit 単位の一定速度で漏れ続ける。注いだら溢れる
// (水位+1 が容量を超える)場合は拒否する。
//
// 水位を tokens = capacity − level と読み替えるとトークンバケットと同じ数式に
// なる(双対関係)。違いが出るのは容量の使い方で、容量を小さく設定する
// (極端には1にする)ことでリクエスト間隔を強制し、流量を平滑化する用途に使う。
// 詳細は docs/06-leaky-bucket.md を参照。
type LeakyBucket struct {
	limit    int           // window あたりの漏れ量(処理レートの分子)
	window   time.Duration // 処理レートの分母
	capacity float64       // バケツの容量
	clock    Clock

	mu     sync.Mutex
	states map[string]*lbState

	stop     chan struct{}
	stopOnce sync.Once
}

// lbState は1つの key のバケツの状態。
// 水位は「最後に見た時刻」と組で持ち、アクセス時に経過時間分をまとめて
// 漏らす(遅延計算)。トークンバケットの補充と対称の構造。
type lbState struct {
	level float64   // 現在の水位
	last  time.Time // 最後に漏れ計算をした時刻
}

var _ Limiter = (*LeakyBucket)(nil)

// NewLeakyBucket は「window あたり limit 回の速度で漏れる、容量 capacity の
// バケツ」によるリミッターを生成する。capacity は WithBurst で指定し、
// 未指定なら limit と同じ。
//
// capacity を 1 にすると「リクエスト間隔は最低 window/limit」という
// 完全な平滑化になる。
//
// 使い終わったら Close を呼んでジャニターを停止すること。
func NewLeakyBucket(limit int, window time.Duration, opts ...Option) (*LeakyBucket, error) {
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
	capacity := cfg.burst
	if capacity == 0 {
		capacity = limit
	}
	if capacity < 0 {
		return nil, fmt.Errorf("ratelimit: burst (capacity) must be positive, got %d", capacity)
	}

	lb := &LeakyBucket{
		limit:    limit,
		window:   window,
		capacity: float64(capacity),
		clock:    cfg.clock,
		states:   make(map[string]*lbState),
		stop:     make(chan struct{}),
	}
	if cfg.cleanupInterval > 0 {
		go runJanitor(lb.stop, cfg.cleanupInterval, func() {
			lb.deleteExpired(lb.clock.Now())
		})
	}
	return lb, nil
}

// leakedIn は経過時間 d の間に漏れる水量。
// 除算を最後に行うことで浮動小数点誤差を最小にする(tokenbucket.go の
// tokensIn と同じ理由)。
func (lb *LeakyBucket) leakedIn(d time.Duration) float64 {
	return float64(d) * float64(lb.limit) / float64(lb.window)
}

// durationFor は水が n 単位漏れるのにかかる時間(ナノ秒切り上げ)。
func (lb *LeakyBucket) durationFor(n float64) time.Duration {
	return time.Duration(math.Ceil(n * float64(lb.window) / float64(lb.limit)))
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (lb *LeakyBucket) Allow(ctx context.Context, key string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := lb.clock.Now()

	lb.mu.Lock()
	defer lb.mu.Unlock()

	st, ok := lb.states[key]
	if !ok {
		// 新しい key は空のバケツから始まる。
		st = &lbState{last: now}
		lb.states[key] = st
	} else {
		// 前回からの経過時間分をまとめて漏らす。水位は0未満にならない。
		st.level = math.Max(0, st.level-lb.leakedIn(now.Sub(st.last)))
		st.last = now
	}

	if st.level+1 > lb.capacity {
		// 溢れる。水位が capacity−1 まで下がれば1単位注げる。
		retry := lb.durationFor(st.level + 1 - lb.capacity)
		return Result{
			Allowed:    false,
			Limit:      int(lb.capacity),
			Remaining:  0,
			RetryAfter: retry,
			ResetAt:    now.Add(lb.timeToEmpty(st.level)),
		}, nil
	}

	st.level++
	return Result{
		Allowed:   true,
		Limit:     int(lb.capacity),
		Remaining: int(lb.capacity - st.level),
		ResetAt:   now.Add(lb.timeToEmpty(st.level)),
	}, nil
}

// timeToEmpty は現在の水位からバケツが空になるまでの時間。
func (lb *LeakyBucket) timeToEmpty(level float64) time.Duration {
	if level <= 0 {
		return 0
	}
	return lb.durationFor(level)
}

// Close はバックグラウンドのジャニターを停止する。複数回呼んでも安全。
func (lb *LeakyBucket) Close() error {
	lb.stopOnce.Do(func() { close(lb.stop) })
	return nil
}

// deleteExpired は水が完全に漏れきった key を削除する。
// 空のバケツは新規 key と区別がつかないため、消しても判定に影響しない。
func (lb *LeakyBucket) deleteExpired(now time.Time) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for key, st := range lb.states {
		if st.level-lb.leakedIn(now.Sub(st.last)) <= 0 {
			delete(lb.states, key)
		}
	}
}
