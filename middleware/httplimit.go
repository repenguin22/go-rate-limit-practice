// Package middleware は ratelimit.Limiter を net/http サーバーに組み込む
// ミドルウェアを提供する。
//
// 拒否時は 429 Too Many Requests と Retry-After ヘッダを返し、すべての
// レスポンスに X-RateLimit-* ヘッダを付与する。リミッターのバックエンド障害時の
// 挙動(fail-open / fail-close)は選択できる。
package middleware

import (
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"

	"github.com/repenguin22/go-rate-limit-practice/ratelimit"
)

// KeyFunc はリクエストからレート制限の単位となる key を取り出す。
// 「誰ごとに制限するか」はアプリケーションの決定なので、ミドルウェアには
// 関数として注入する。
type KeyFunc func(*http.Request) (string, error)

// KeyByIP は接続元IPアドレスを key にする KeyFunc。
//
// r.RemoteAddr(実際のTCP接続の相手)のみを使い、X-Forwarded-For などの
// ヘッダは参照しない。ヘッダはクライアントが自由に偽装できるため、
// 信頼できるプロキシ配下で使う場合はプロキシが検証済みの値を渡す
// 専用の KeyFunc を書くこと(docs/07-http-middleware.md 参照)。
func KeyByIP(r *http.Request) (string, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "", err
	}
	return host, nil
}

// KeyByHeader は指定ヘッダの値(APIキーなど)を key にする KeyFunc を返す。
// ヘッダが空の場合はエラーを返し、リクエストは 400 で拒否される。
// 認証を伴う場合は、認証ミドルウェアを先に通した上で検証済みの値を使うこと。
func KeyByHeader(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		v := r.Header.Get(name)
		if v == "" {
			return "", errors.New("middleware: missing header " + name)
		}
		return v, nil
	}
}

// FailurePolicy はリミッターがエラーを返したとき(Redis障害など)の挙動。
type FailurePolicy int

const (
	// FailOpen はリクエストを制限なしで通す(可用性優先)。
	// レートリミットは本来「あったほうがよい保護」なので、リミッターの
	// 障害でサービス全体を止めないこちらがデフォルト。
	FailOpen FailurePolicy = iota

	// FailClose は 503 Service Unavailable で拒否する(保護優先)。
	// ブルートフォース対策など、制限が効かない状態で通すことが
	// 許されない用途で使う。
	FailClose
)

// Middleware はレートリミットを行う http ミドルウェア。
type Middleware struct {
	limiter ratelimit.Limiter
	key     KeyFunc
	policy  FailurePolicy
	logger  *slog.Logger
}

// Option は Middleware 生成時の追加設定。
type Option func(*Middleware)

// WithFailurePolicy はリミッター障害時の挙動を設定する。デフォルトは FailOpen。
func WithFailurePolicy(p FailurePolicy) Option {
	return func(m *Middleware) { m.policy = p }
}

// WithLogger は障害ログの出力先を設定する。デフォルトは slog.Default()。
func WithLogger(l *slog.Logger) Option {
	return func(m *Middleware) { m.logger = l }
}

// New はレートリミットミドルウェアを生成する。
func New(limiter ratelimit.Limiter, key KeyFunc, opts ...Option) (*Middleware, error) {
	if limiter == nil {
		return nil, errors.New("middleware: limiter must not be nil")
	}
	if key == nil {
		return nil, errors.New("middleware: key func must not be nil")
	}
	m := &Middleware{
		limiter: limiter,
		key:     key,
		policy:  FailOpen,
		logger:  slog.Default(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m, nil
}

// Wrap は next をレートリミット付きのハンドラーに包んで返す。
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := m.key(r)
		if err != nil {
			// key が特定できないリクエストを通すと制限を素通りする
			// 抜け道になるため、こちらは常に拒否する。
			m.logger.WarnContext(r.Context(), "ratelimit: cannot determine key", "error", err)
			http.Error(w, "cannot determine rate limit key", http.StatusBadRequest)
			return
		}

		res, err := m.limiter.Allow(r.Context(), key)
		if err != nil {
			m.logger.ErrorContext(r.Context(), "ratelimit: limiter failed", "error", err, "policy", m.policy)
			if m.policy == FailClose {
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r) // FailOpen: 制限なしで通す
			return
		}

		setRateLimitHeaders(w, res)
		if !res.Allowed {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(res)))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// setRateLimitHeaders は判定結果を X-RateLimit-* ヘッダとして付与する。
// 慣習として広く使われている形式(GitHub, Stripe 等)。IETF で
// RateLimit ヘッダとして標準化が進行中だが、まだドラフト段階。
func setRateLimitHeaders(w http.ResponseWriter, res ratelimit.Result) {
	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetAt.Unix(), 10))
}

// retryAfterSeconds は RetryAfter を Retry-After ヘッダ用の秒数に変換する。
// HTTP の Retry-After は整数秒なので切り上げる(切り捨てると「案内どおり
// 待ったのにまだ拒否される」が起きる)。最低1秒。
func retryAfterSeconds(res ratelimit.Result) int {
	s := int(math.Ceil(res.RetryAfter.Seconds()))
	if s < 1 {
		s = 1
	}
	return s
}
