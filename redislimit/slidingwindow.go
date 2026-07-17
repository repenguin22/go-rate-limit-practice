package redislimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/repenguin22/go-rate-limit-practice/ratelimit"
)

// slidingWindowScript はインメモリ版 SlidingWindowCounter の Allow と同じ
// ロジックを Redis 上で原子的に実行する(ratelimit/slidingwindowcounter.go と
// 見比べながら読むこと)。
//
// 状態は1つのハッシュキーに ws(現ウィンドウ開始)/prev/curr の3フィールドで
// 持つ。1キーに収めることで Redis Cluster でも安全に動く(複数キーに分けると
// 別ノードに配置されて Lua から操作できない)。
//
// 時刻は Redis サーバーの TIME を使う。全インスタンスが同じ時計を見るため、
// アプリサーバー間のクロックスキューが判定に影響しない。
var slidingWindowScript = redis.NewScript(`
-- KEYS[1]: 状態キー(hash: ws, prev, curr)
-- ARGV[1]: limit, ARGV[2]: window(ミリ秒)
-- 返り値: {allowed(0/1), remaining, retry_ms, reset_ms}
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local start = now - (now % window)

local state = redis.call('HMGET', KEYS[1], 'ws', 'prev', 'curr')
local ws = tonumber(state[1])
local prev = tonumber(state[2])
local curr = tonumber(state[3])
if ws == nil then
  ws, prev, curr = start, 0, 0
elseif ws ~= start then
  -- ウィンドウの切り替わり。1つ先ならシフト、2つ以上先なら全リセット。
  if ws == start - window then
    prev = curr
  else
    prev = 0
  end
  curr = 0
  ws = start
end

local elapsed = now - start
local weight = 1 - elapsed / window
local est = prev * weight + curr

local allowed = 0
local retry = 0
if est < limit then
  allowed = 1
  curr = curr + 1
else
  if prev > 0 and curr < limit then
    retry = math.ceil(window * (prev + curr - limit) / prev) + 1 - elapsed
    if retry < 0 then retry = 0 end
  else
    retry = window - elapsed
  end
end

redis.call('HSET', KEYS[1], 'ws', ws, 'prev', prev, 'curr', curr)
-- curr は次ウィンドウでも prev として参照されるため 2 ウィンドウ分残す
-- (インメモリ版の deleteExpired と同じ理由)。
redis.call('PEXPIRE', KEYS[1], 2 * window)

local remaining = limit - math.ceil(est + 1)
if allowed == 0 or remaining < 0 then
  remaining = 0
end
return {allowed, remaining, retry, window - elapsed}
`)

// SlidingWindow はスライディングウィンドウカウンタの Redis バックエンド実装。
// key あたりの状態が小さく(3フィールド)、Redis の負荷とメモリを抑えつつ
// 境界バーストを防げるため、分散レートリミットの実用的な第一候補。
type SlidingWindow struct {
	client Scripter
	limit  int
	window time.Duration
	prefix string
}

var _ ratelimit.Limiter = (*SlidingWindow)(nil)

// NewSlidingWindow は Redis を使うスライディングウィンドウカウンタ
// リミッターを生成する。状態は TTL で自動的に消えるため、Close は不要。
func NewSlidingWindow(client Scripter, limit int, window time.Duration, opts ...Option) (*SlidingWindow, error) {
	if err := validate(client, limit, window); err != nil {
		return nil, err
	}
	cfg := config{prefix: "ratelimit:sliding:"}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &SlidingWindow{client: client, limit: limit, window: window, prefix: cfg.prefix}, nil
}

// Allow は key に対するリクエスト1件を許可できるか判定する。
func (s *SlidingWindow) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	vals, err := runScript(ctx, s.client, slidingWindowScript,
		[]string{s.prefix + key}, s.limit, s.window.Milliseconds())
	if err != nil {
		return ratelimit.Result{}, err
	}
	return ratelimit.Result{
		Allowed:    vals[0] == 1,
		Limit:      s.limit,
		Remaining:  int(vals[1]),
		RetryAfter: time.Duration(vals[2]) * time.Millisecond,
		ResetAt:    time.Now().Add(time.Duration(vals[3]) * time.Millisecond),
	}, nil
}
