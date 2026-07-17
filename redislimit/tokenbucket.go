package redislimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/repenguin22/go-rate-limit-practice/ratelimit"
)

// tokenBucketScript はインメモリ版 TokenBucket の Allow と同じロジックを
// Redis 上で原子的に実行する(ratelimit/tokenbucket.go と見比べながら読むこと)。
//
// 遅延補充・容量頭打ち・除算を最後にする浮動小数点対策まで、構造は
// インメモリ版と1対1に対応する。Lua の number は float64 なので、
// トークンの端数もそのまま表現できる。
var tokenBucketScript = redis.NewScript(`
-- KEYS[1]: 状態キー(hash: tokens, last)
-- ARGV[1]: limit, ARGV[2]: window(ミリ秒), ARGV[3]: burst
-- 返り値: {allowed(0/1), remaining, retry_ms, reset_ms}
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])

local state = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(state[1])
local last = tonumber(state[2])
if tokens == nil then
  -- 新しい key はバケツ満タンから始まる。
  tokens = burst
  last = now
end

-- 遅延補充。除算は最後に(浮動小数点誤差対策、インメモリ版と同じ)。
tokens = tokens + (now - last) * limit / window
if tokens > burst then
  tokens = burst
end

local allowed = 0
local retry = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
else
  retry = math.ceil((1 - tokens) * window / limit)
end

local full = math.ceil((burst - tokens) * window / limit)
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last', now)
-- 満タンに戻った状態は新規 key と区別がつかないので、そこまでの時間を TTL に
-- する(インメモリ版の deleteExpired と同じ理由)。
redis.call('PEXPIRE', KEYS[1], math.max(full, 1))
return {allowed, math.floor(tokens), retry, full}
`)

// TokenBucket はトークンバケットの Redis バックエンド実装。
// 平均レートとバースト許容量を分散環境全体で共有できる。
type TokenBucket struct {
	client Scripter
	limit  int
	window time.Duration
	burst  int
	prefix string
}

var _ ratelimit.Limiter = (*TokenBucket)(nil)

// NewTokenBucket は Redis を使うトークンバケットリミッターを生成する。
// burst(バケツ容量)はデフォルトで limit と同じ。WithBurst で変更できる。
// 状態は TTL で自動的に消えるため、Close は不要。
func NewTokenBucket(client Scripter, limit int, window time.Duration, opts ...Option) (*TokenBucket, error) {
	if err := validate(client, limit, window); err != nil {
		return nil, err
	}
	cfg := config{prefix: "ratelimit:token:", burst: limit}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.burst <= 0 {
		return nil, errBurst(cfg.burst)
	}
	return &TokenBucket{
		client: client,
		limit:  limit,
		window: window,
		burst:  cfg.burst,
		prefix: cfg.prefix,
	}, nil
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (b *TokenBucket) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	vals, err := runScript(ctx, b.client, tokenBucketScript,
		[]string{b.prefix + key}, b.limit, b.window.Milliseconds(), b.burst)
	if err != nil {
		return ratelimit.Result{}, err
	}
	return ratelimit.Result{
		Allowed:    vals[0] == 1,
		Limit:      b.burst,
		Remaining:  int(vals[1]),
		RetryAfter: time.Duration(vals[2]) * time.Millisecond,
		ResetAt:    time.Now().Add(time.Duration(vals[3]) * time.Millisecond),
	}, nil
}
