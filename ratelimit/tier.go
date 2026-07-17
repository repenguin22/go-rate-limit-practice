package ratelimit

import (
	"context"
	"fmt"
	"strings"
)

// TierSeparator は TierLimiter が key を「tier 部」と「クライアント部」に
// 分割する区切り文字。
const TierSeparator = ":"

// TierKey は TierLimiter に渡す key を組み立てる。
// HTTPミドルウェアの KeyFunc などで使う:
//
//	// Auth0 のカスタムクレームから tier と client_id を取り出した後
//	return ratelimit.TierKey(claims.RateTier, claims.ClientID), nil
func TierKey(tier, key string) string {
	return tier + TierSeparator + key
}

// TierLimiter は key の tier 接頭辞(プラン名など)を見て、tier ごとに
// 異なる Limiter へ振り分ける合成リミッター。
// 「無料プランは 60回/分、有料プランは 600回/分」のように、同じ API に対して
// クライアントの契約ごとに異なる制限をかける用途に使う。
//
// key は "tier:clientID" 形式(TierKey で組み立てる)。tier 部が limiters に
// 見つかればそのリミッターへクライアント部を key として委譲する。
// tier が未知、または区切り文字がない場合は fallback へ元の key 全体を渡す
// (元の key を保つことで、異なる未知 tier 同士が状態を共有しない)。
//
// tier の判定は呼び出し側の責任で「検証済みの値」から行うこと。
// クライアントが自称する tier をそのまま使うと、制限の緩い tier を
// 名乗るだけで制限をすり抜けられる。
type TierLimiter struct {
	limiters map[string]Limiter
	fallback Limiter
}

var _ Limiter = (*TierLimiter)(nil)

// NewTierLimiter は tier 名 → Limiter の対応表と、未知の tier に適用する
// fallback からティア別リミッターを生成する。
//
// fallback は必須。「未知 tier は無制限」は設定ミス一つで制限が消える罠に、
// 「未知 tier は全拒否」は設定ミス一つで障害になるため、どちらもデフォルトに
// しない。安全側の既定(たとえば最も厳しいプラン相当)を呼び出し側が明示する。
func NewTierLimiter(limiters map[string]Limiter, fallback Limiter) (*TierLimiter, error) {
	if fallback == nil {
		return nil, fmt.Errorf("ratelimit: fallback limiter must not be nil")
	}
	m := make(map[string]Limiter, len(limiters))
	for tier, lim := range limiters {
		if lim == nil {
			return nil, fmt.Errorf("ratelimit: limiter for tier %q must not be nil", tier)
		}
		m[tier] = lim
	}
	return &TierLimiter{limiters: m, fallback: fallback}, nil
}

// Allow は key の tier 部に対応するリミッターへ判定を委譲する。
func (t *TierLimiter) Allow(ctx context.Context, key string) (Result, error) {
	tier, rest, found := strings.Cut(key, TierSeparator)
	if found {
		if lim, ok := t.limiters[tier]; ok {
			return lim.Allow(ctx, rest)
		}
	}
	return t.fallback.Allow(ctx, key)
}

// Close は内包するリミッターのうち Close を持つものをすべて閉じる。
// 同じインスタンスが複数の tier に登録されていても一度しか閉じない。
// 最初に発生したエラーを返す(残りも閉じ続ける)。
func (t *TierLimiter) Close() error {
	closed := make(map[Limiter]bool)
	var firstErr error
	closeOne := func(lim Limiter) {
		if closed[lim] {
			return
		}
		closed[lim] = true
		if c, ok := lim.(interface{ Close() error }); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, lim := range t.limiters {
		closeOne(lim)
	}
	closeOne(t.fallback)
	return firstErr
}
