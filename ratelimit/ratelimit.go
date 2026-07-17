// Package ratelimit はレートリミットの主要アルゴリズムのインメモリ実装を提供する。
//
// すべてのリミッターは [Limiter] インターフェースを満たし、key(ユーザーID・
// IPアドレスなど)ごとに独立したレート制限を行う。実装はすべてゴルーチンセーフである。
//
// 分散環境で制限を共有したい場合は redislimit パッケージを使う。
package ratelimit

import (
	"context"
	"time"
)

// Limiter はレートリミッターの共通インターフェース。
//
// インメモリ実装はエラーを返さないが、分散実装(Redisなど)はネットワークI/Oを
// 伴うため、インターフェースとしては context とエラーを最初から受け渡しする。
// これにより呼び出し側はバックエンドがどこにあるかを意識せず差し替えられる。
type Limiter interface {
	// Allow は key に対するリクエスト1件を許可できるか判定し、結果を返す。
	// 判定できた場合(許可・拒否どちらでも)エラーは nil。
	// エラーが非 nil の場合、Result の内容は意味を持たない。
	Allow(ctx context.Context, key string) (Result, error)
}

// Result はレート制限の判定結果。
// HTTP レスポンスの RateLimit-* / Retry-After ヘッダにそのまま使える情報を含む。
type Result struct {
	// Allowed はこのリクエストが許可されたかどうか。
	Allowed bool

	// Limit は設定されている上限値(ウィンドウあたりの許可数、またはバーストサイズ)。
	Limit int

	// Remaining は現時点で追加で許可できる残り回数。拒否時は 0。
	Remaining int

	// RetryAfter は拒否されたリクエストが次に許可されるまでの待ち時間。
	// 許可された場合は 0。
	RetryAfter time.Duration

	// ResetAt は制限状態がリセットされる(または枠が回復し始める)時刻。
	ResetAt time.Time
}

// Clock は現在時刻の取得を抽象化する。
// テストでは固定・手動進行のクロックを注入することで、実時間を待たずに
// 「10秒後」の挙動を決定的に検証できる。
type Clock interface {
	Now() time.Time
}

// systemClock は time.Now をそのまま使う本番用クロック。
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock は time.Now を使うデフォルトのクロックを返す。
func SystemClock() Clock { return systemClock{} }
