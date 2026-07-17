package redislimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/repenguin22/go-rate-limit-practice/ratelimit"
)

// fixedWindowScript は INCR + PEXPIRE を原子的に行う。
//
// INCR と EXPIRE を別コマンドで送ると、INCR 直後にクライアントが落ちた場合に
// TTL のないキーが永久に残る(メモリリーク+そのキーは二度とリセットされない)。
// Lua で1スクリプトにまとめることで「カウントしたのに期限がない」状態を
// 構造的に排除する。
//
// なおウィンドウは壁時計ではなく「そのキーへの最初のリクエスト」から始まる
// (TTLベースの自然な実装)。インメモリ版(壁時計整列)との違いに注意。
var fixedWindowScript = redis.NewScript(`
-- KEYS[1]: カウンタキー
-- ARGV[1]: limit, ARGV[2]: window(ミリ秒)
-- 返り値: {allowed(0/1), count, ttl_ms}
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
local ttl = redis.call('PTTL', KEYS[1])
local allowed = 0
if count <= tonumber(ARGV[1]) then
  allowed = 1
end
return {allowed, count, ttl}
`)

// FixedWindow は固定ウィンドウカウンタの Redis バックエンド実装。
// 分散レートリミットの入門としてもっとも単純な形。
type FixedWindow struct {
	client Scripter
	limit  int
	window time.Duration
	prefix string
}

var _ ratelimit.Limiter = (*FixedWindow)(nil)

// NewFixedWindow は Redis を使う固定ウィンドウリミッターを生成する。
// 状態は TTL で自動的に消えるため、Close は不要。
func NewFixedWindow(client Scripter, limit int, window time.Duration, opts ...Option) (*FixedWindow, error) {
	if err := validate(client, limit, window); err != nil {
		return nil, err
	}
	cfg := config{prefix: "ratelimit:fixed:"}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &FixedWindow{client: client, limit: limit, window: window, prefix: cfg.prefix}, nil
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
// Redis に到達できない場合はエラーを返す(fail-open/close は呼び出し側の判断)。
func (f *FixedWindow) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	vals, err := runScript(ctx, f.client, fixedWindowScript,
		[]string{f.prefix + key}, f.limit, f.window.Milliseconds())
	if err != nil {
		return ratelimit.Result{}, err
	}
	allowed, count, ttlMs := vals[0] == 1, int(vals[1]), vals[2]

	ttl := time.Duration(ttlMs) * time.Millisecond
	res := ratelimit.Result{
		Allowed:   allowed,
		Limit:     f.limit,
		Remaining: max(0, f.limit-count),
		ResetAt:   time.Now().Add(ttl),
	}
	if !allowed {
		res.RetryAfter = ttl
	}
	return res, nil
}
