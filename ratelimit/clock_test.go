package ratelimit

import (
	"sync"
	"time"
)

// fakeClock はテスト用の手動進行クロック。
// Advance で時間を進めることで、実時間を待たずに時間経過をテストできる。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// newFakeClock は基準時刻(2026-01-01 00:00:00 UTC)から始まるクロックを返す。
// ウィンドウ境界の計算が読みやすいよう、キリのいい時刻を使う。
func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance は現在時刻を d だけ進める。
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
