package ratelimit

import "time"

// config は各リミッター共通の生成時設定。
type config struct {
	clock           Clock
	cleanupInterval time.Duration
	burst           int // 0 は「未指定」(各リミッターのデフォルトを使う)
}

// defaultConfig はデフォルト設定を返す。
// クリーンアップ間隔はウィンドウ幅と同じ(ただし最短1秒)とする。
// 間隔を短くしすぎるとロック競合が増えるだけで得るものがないため。
func defaultConfig(window time.Duration) config {
	interval := window
	if interval < time.Second {
		interval = time.Second
	}
	return config{
		clock:           SystemClock(),
		cleanupInterval: interval,
	}
}

// Option はリミッター生成時の追加設定。
type Option func(*config)

// WithClock は時刻の取得元を差し替える。主にテストで使う。
func WithClock(c Clock) Option {
	return func(cfg *config) { cfg.clock = c }
}

// WithCleanupInterval は期限切れ key を掃除するジャニターの実行間隔を
// 変更する。0 以下を指定するとジャニターを起動しない(呼び出し側が
// メモリ管理に責任を持つ場合や、短命なテスト用)。
func WithCleanupInterval(d time.Duration) Option {
	return func(cfg *config) { cfg.cleanupInterval = d }
}

// WithBurst はバケット容量(瞬間的に受け入れられるリクエスト数)を変更する。
// TokenBucket / LeakyBucket 専用で、他のリミッターでは無視される。
// 未指定の場合、容量は limit と同じになる。
func WithBurst(n int) Option {
	return func(cfg *config) { cfg.burst = n }
}
