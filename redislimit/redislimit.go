// Package redislimit はレートリミットの Redis バックエンド実装(分散版)を提供する。
//
// 複数のアプリケーションインスタンスが同じ Redis を参照することで、
// インスタンスをまたいで1つの制限を共有できる。すべての実装は
// ratelimit.Limiter インターフェースを満たすため、インメモリ実装と
// そのまま差し替えられる。
//
// 設計上のポイント(詳細は docs/09-distributed-redis.md):
//
//   - 判定は1つの Lua スクリプトで原子的に行う。GET してから SET する方式では
//     並行アクセス時に check-and-set の競合が起きる
//   - 時刻は Redis サーバーの TIME コマンドから取得する。アプリ側の時計を
//     使うとインスタンス間のクロックスキューがそのまま判定誤差になる
//   - 状態の掃除はジャニターではなく Redis の TTL(PEXPIRE)に任せる
//   - Redis 障害時はエラーを返す。通すか止めるかは呼び出し側
//     (middleware の FailurePolicy)が決める
package redislimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Scripter は必要な Redis 操作(Luaスクリプト実行)の最小インターフェース。
// *redis.Client / *redis.ClusterClient / redis.UniversalClient すべてが満たす。
type Scripter = redis.Scripter

// config は各リミッター共通の生成時設定。
type config struct {
	prefix string
	burst  int
}

// Option はリミッター生成時の追加設定。
type Option func(*config)

// WithKeyPrefix は Redis キーの接頭辞を変更する。
// 同じ Redis を複数の用途で共有する場合の衝突回避に使う。
func WithKeyPrefix(p string) Option {
	return func(cfg *config) { cfg.prefix = p }
}

// WithBurst はバケツ容量を変更する。TokenBucket 専用で、他では無視される。
// 未指定の場合、容量は limit と同じになる。
func WithBurst(n int) Option {
	return func(cfg *config) { cfg.burst = n }
}

func errBurst(n int) error {
	return fmt.Errorf("redislimit: burst must be positive, got %d", n)
}

// validate は全実装共通のパラメータ検証。
func validate(client Scripter, limit int, window time.Duration) error {
	if client == nil {
		return fmt.Errorf("redislimit: client must not be nil")
	}
	if limit <= 0 {
		return fmt.Errorf("redislimit: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return fmt.Errorf("redislimit: window must be positive, got %v", window)
	}
	return nil
}

// runScript はスクリプトを実行し、返り値を int64 の配列として取り出す。
// redis.NewScript は EVALSHA を試み、未ロードなら自動で EVAL にフォールバック
// する(スクリプトの往復転送を初回だけにする最適化)。
func runScript(ctx context.Context, client Scripter, script *redis.Script, keys []string, args ...any) ([]int64, error) {
	res, err := script.Run(ctx, client, keys, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("redislimit: script failed: %w", err)
	}
	raw, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redislimit: unexpected script result type %T", res)
	}
	out := make([]int64, len(raw))
	for i, v := range raw {
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("redislimit: unexpected script result element %d: %T", i, v)
		}
		out[i] = n
	}
	return out, nil
}
