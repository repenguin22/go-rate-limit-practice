# go-rate-limit — Goで学ぶレートリミット

レートリミットの主要アルゴリズムを Go で一から実装しながら学ぶプロジェクト。
コードはプロダクションレディな品質(ゴルーチンセーフ、テスト・ベンチマーク完備、
リソース管理、エラー処理)で書かれており、実務の参考にもなる。

## 学習の進め方

各 Stage は「解説ドキュメントを読む → 実装とテストを読む → 演習」の順で進める。

| Stage | ドキュメント | 実装 | 状態 |
|-------|------------|------|------|
| — | [要件定義](docs/00-requirements.md) | — | ✅ |
| 0 | [レートリミットとは・共通設計](docs/01-what-is-rate-limiting.md) | [ratelimit.go](ratelimit/ratelimit.go) | ✅ |
| 1 | [固定ウィンドウカウンタ](docs/02-fixed-window.md) | [fixedwindow.go](ratelimit/fixedwindow.go) | ✅ |
| 2 | [スライディングウィンドウログ](docs/03-sliding-window-log.md) | [slidingwindowlog.go](ratelimit/slidingwindowlog.go) | ✅ |
| 3 | [スライディングウィンドウカウンタ](docs/04-sliding-window-counter.md) | [slidingwindowcounter.go](ratelimit/slidingwindowcounter.go) | ✅ |
| 4 | [トークンバケット](docs/05-token-bucket.md) | [tokenbucket.go](ratelimit/tokenbucket.go) | ✅ |
| 5 | [リーキーバケット](docs/06-leaky-bucket.md) | [leakybucket.go](ratelimit/leakybucket.go) | ✅ |
| 6 | [HTTPミドルウェア](docs/07-http-middleware.md) | [httplimit.go](middleware/httplimit.go) | ✅ |
| 7 | [x/time/rate との比較](docs/08-x-time-rate.md) | [xtimerate_test.go](ratelimit/xtimerate_test.go) | ✅ |
| 8 | [分散レートリミット (Redis)](docs/09-distributed-redis.md) | [redislimit/](redislimit/) | ✅ |
| 付録 | [実務ガイド(本番での選択・gRPC-gateway への組み込み)](docs/10-production-guide.md) | — | ✅ |

## 使い方(現時点のAPI)

```go
import "github.com/repenguin22/go-rate-limit-practice/ratelimit"

// 1分間に60回まで
limiter, err := ratelimit.NewFixedWindow(60, time.Minute)
if err != nil {
	log.Fatal(err)
}
defer limiter.Close()

res, err := limiter.Allow(ctx, clientIP)
if err != nil {
	// バックエンド障害など(インメモリ実装では context キャンセルのみ)
}
if !res.Allowed {
	// 429 を返す。res.RetryAfter / res.Remaining が使える
}
```

## デモサーバー

```sh
go run ./cmd/demo -algo token -limit 5 -window 10s
# 別ターミナルで:
for i in $(seq 1 8); do curl -s -i localhost:8080/ | head -n 6; echo; done
```

`-algo fixed | swlog | swcounter | token | leaky` で切り替えて挙動の違いを観察できる。

## 開発コマンド

```sh
go test -race ./...              # 全テスト(データ競合検出付き)
go test -bench . -benchmem ./... # ベンチマーク
go vet ./...                     # 静的検査
```
