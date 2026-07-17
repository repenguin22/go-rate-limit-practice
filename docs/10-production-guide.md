# 付録: 実務ガイド — 本番でのアルゴリズム選択と gRPC-gateway への組み込み

全ステージを終えた人向けの「で、実務ではどうするの?」への回答。
前半は一般論(FAQ)、後半は Go の gRPC-gateway + Redis 構成での具体的な設計。

## 1. FAQ: エンタープライズでは結局どのアルゴリズムを使うのか

**単一の正解はないが、用途ごとの定番ははっきりある。** 大手の実例:

| 事例 | アルゴリズム | 対応する Stage |
|---|---|---|
| Stripe | トークンバケット + Redis (Lua) | Stage 4 + 8 |
| Cloudflare | スライディングウィンドウカウンタ | Stage 3 |
| GitHub API | 固定ウィンドウ(毎時リセットのクォータ) | Stage 1 |
| AWS API Gateway | トークンバケット(rate と burst を別設定) | Stage 4 |
| nginx `limit_req` | リーキーバケット(burst キュー付き) | Stage 5 |
| go-redis 公式 redis_rate | GCRA(トークンバケットの変種) | Stage 8 §6 |

本プロジェクトで作った5方式はすべて現役。選ばれ方の傾向:

- **流量制限(秒〜分)** → トークンバケット / スライディングウィンドウカウンタ / GCRA。
  性能はどれも問題にならないので、バースト許容が要るか(トークン)、
  挙動の説明しやすさ(ウィンドウ)で選ぶ
- **クォータ(時〜日の契約上限)** → 固定ウィンドウ。「毎時0分にリセット」は
  ユーザーに説明しやすく、境界バーストも日単位では実害が小さい
- 実務では**重ねがけが普通**: 「100回/秒(トークンバケット)かつ
  10万回/日(固定ウィンドウ)」のように役割の違う制限を積む

### 本当に効くのはアルゴリズムよりレイヤリング

```
インターネット
  │
  ▼ ① エッジ / WAF(Cloudflare, ALB…)     … IPベースの粗い防御。自分では書かない
  ▼ ② APIゲートウェイ(Kong, Envoy…)       … APIキー単位。Redisバックエンド
  ▼ ③ アプリ内                             … プラン別クォータ等、ビジネスと結合した制限
  ▼ ④ 送信側(自分が外部APIを呼ぶとき)      … x/time/rate の独壇場
```

②が既にあるなら③で汎用の流量制限を再実装しない(二重管理になる)。
③に置くべきなのは「プラン・課金・テナントと結合した制限」だけ。
そして障害・インシデントに直結するのはアルゴリズムではなく運用面
(fail-open/close、Retry-After、key の選び方、Redis 往復のレイテンシ)。

### x/time/rate はそのまま使えるのか

- **使える(第一選択)**: ④送信側の自己制御(`Wait(ctx)`)、単一プロセス内の保護。
  Kubernetes のコントローラ類が大量に使っているのがこれ
- **そのままでは使えない**: サーバー側のユーザー単位制限。インスタンス間で
  共有されず(10台なら実質10倍)、再起動でリセットされ、key の概念がなく、
  Retry-After 用のメタデータも取りにくい
- **割り切りとしてアリ**: `map[string]*rate.Limiter` を各インスタンスに持ち
  「制限 × 台数」までの漏れを許容する構成。Redis 往復コストがゼロになるので
  社内サービス等では現実的

## 2. gRPC-gateway + Redis 構成での実装

gRPC-gateway は「HTTP/JSON を受けて gRPC サービスへプロキシする
`http.Handler`」なので、レートリミットを挟める場所が2つある:

```
クライアント ──HTTP──▶ [gRPC-gateway] ──gRPC──▶ [gRPCサーバー]
                        ▲ 選択肢A                ▲ 選択肢B
                        HTTPミドルウェア          インターセプタ
                        (Stage 6 がそのまま使える)
```

### 2.1 選択肢A: gateway の HTTP 層で制限する

gateway の `runtime.ServeMux` はただの `http.Handler`。
**Stage 6 のミドルウェアが無改造でそのまま使える**:

```go
gwmux := runtime.NewServeMux()
// ... RegisterXxxServiceHandler(ctx, gwmux, conn) ...

rdb := redis.NewClient(&redis.Options{Addr: "redis:6379"})
lim, err := redislimit.NewTokenBucket(rdb, 100, time.Minute, redislimit.WithBurst(20))
if err != nil { ... }
mw, err := middleware.New(lim, keyFromAuthHeader,
	middleware.WithFailurePolicy(middleware.FailOpen))
if err != nil { ... }

srv := &http.Server{Addr: ":8080", Handler: mw.Wrap(gwmux)}
```

向いているケース: 外部トラフィックが全部 gateway 経由で入ってくる構成。
429 / Retry-After / X-RateLimit-* ヘッダが自然に返せるのも利点。

### 2.2 選択肢B: gRPC インターセプタで制限する

gRPC サーバーが gateway 以外からも直接呼ばれる(社内の別サービス、
モバイルの gRPC 直結など)なら、HTTP 層では守り切れない。
gRPC の unary interceptor に組み込む:

```go
// UnaryRateLimit は Limiter を gRPC unary interceptor に適合させる。
// keyFn はメタデータ等から制限単位の key を取り出す。
func UnaryRateLimit(lim ratelimit.Limiter, keyFn func(ctx context.Context, fullMethod string) (string, error)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		key, err := keyFn(ctx, info.FullMethod)
		if err != nil {
			// key 不明は素通りさせない(Stage 6 §2 と同じ理屈)
			return nil, status.Error(codes.Unauthenticated, "cannot determine rate limit key")
		}
		res, err := lim.Allow(ctx, key)
		if err != nil {
			// fail-open。必ずログ+メトリクスを残すこと
			slog.ErrorContext(ctx, "ratelimit: limiter failed", "error", err)
			return handler(ctx, req)
		}
		if !res.Allowed {
			return nil, status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded, retry after %v", res.RetryAfter)
		}
		return handler(ctx, req)
	}
}
```

重要な性質: **gRPC-gateway は `codes.ResourceExhausted` を HTTP 429 に
自動でマッピングする**(`runtime.HTTPStatusFromCode`)。つまりインターセプタで
拒否すれば、gateway 経由のクライアントにはちゃんと 429 が返る。

key の取り出しは gRPC 流になる:

```go
func keyFromMetadata(ctx context.Context, _ string) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("no metadata")
	}
	// gateway は Authorization 等の permanent header をそのまま metadata に
	// 転送する。その他のヘッダは grpcgateway- プレフィックス付きになる。
	if v := md.Get("authorization"); len(v) > 0 {
		return subjectFromToken(v[0]) // 検証済みトークンから subject を取る
	}
	// 接続元IPは peer から。ただし gateway 経由だと gateway のIPになるので、
	// gateway が付与する x-forwarded-for を「gateway を信頼して」使う
	// (Stage 6 §2 の XFF の議論がそのまま適用される)。
	return "", errors.New("unauthenticated")
}
```

### 2.3 A と B の選び方

| | A: HTTP ミドルウェア | B: gRPC インターセプタ |
|---|---|---|
| 守れる経路 | gateway 経由のみ | gateway + 直接 gRPC の両方 |
| 429 ヘッダ(Retry-After 等) | 完全に制御できる | 自動マッピング(ヘッダは工夫が要る、下記) |
| メソッド単位の制限 | パスのルーティングが要る | `info.FullMethod` で自然にできる |
| 実装コスト | Stage 6 がそのまま | interceptor を書く(上記 ~30行) |

実務の推奨は「**両方**」が多い: gateway に粗い全体防御(A)、
gRPC 側にテナント/メソッド単位の細かい制限(B)。Redis は共有できる
(`WithKeyPrefix` で名前空間を分ける)。

### 2.4 gRPC 特有の注意点

- **Retry-After の伝搬**: 素の gRPC に Retry-After ヘッダはない。
  丁寧にやるなら `google.golang.org/genproto/googleapis/rpc/errdetails` の
  `RetryInfo` を status details に付け、gateway 側は
  `runtime.WithErrorHandler` でそれを HTTP の `Retry-After` ヘッダへ変換する。
  gRPC ネイティブのクライアントも `RetryInfo` を見てバックオフできる
- **ストリーミング RPC**: stream interceptor では「ストリーム開始時に1回」
  課金するか「メッセージごと」に課金するかを決める必要がある。
  メッセージごとなら `grpc.ServerStream` をラップして `RecvMsg` 内で
  `Allow` を呼ぶ。長寿命ストリームは開始時1回だと実質ノーガードになる点に注意
- **メソッドごとのコスト差**: 検索系の重い RPC と軽い RPC を同じ1カウントに
  しない。key に `info.FullMethod` を含めて別枠にするか、
  「n トークン消費する `AllowN`」を実装する(Stage 7 演習2の実務版)
- **interceptor の順序**: 認証 → レートリミット → ロギング/リカバリの順が基本。
  認証前に user ID では制限できないし、リミッターより先に重い処理を
  置いたら守る意味がない

### 2.5 運用チェックリスト

- [ ] Redis クライアントのタイムアウトを短く(`DialTimeout`/`ReadTimeout` 数十ms)。
      リミッター判定のためにリクエスト全体を遅らせない
- [ ] fail-open + **エラー率のメトリクスとアラート**。黙って fail-open し続けて
      「リミッターが1週間死んでいた」を防ぐ(Stage 6 §4)
- [ ] ブルートフォース対策系のエンドポイント(ログイン等)だけは fail-close
- [ ] `WithKeyPrefix` を環境・用途ごとに分ける(staging と本番の Redis 共有事故対策)
- [ ] key のカーディナリティを見積もる(テナント数 × メソッド数)。Redis のメモリは
      TTL が守るが、監視はしておく
- [ ] Redis Cluster を使うなら1判定1キーを維持する(本実装は維持済み。Stage 8 §6)
- [ ] 負荷試験でリミッター自体のレイテンシ(Redis 往復)を計測。ボトルネックに
      なるなら「ローカル概算 + 定期同期」のハイブリッドを検討(精度と引き換え)

## 3. まとめ

gRPC-gateway + Redis の分散環境なら:

1. アルゴリズムは **Redis トークンバケット**(Stage 8 実装)か **GCRA** を第一候補に。
   クォータが要件にあるなら固定ウィンドウを重ねる
2. 置き場所は gateway の HTTP ミドルウェア(A)と gRPC インターセプタ(B)の
   併用。拒否は `codes.ResourceExhausted` に統一すれば HTTP 429 への変換は
   gateway が面倒を見てくれる
3. 差がつくのは運用面: fail-open + アラート、短いタイムアウト、`RetryInfo` の伝搬、
   ストリーミングの課金単位。ここは Stage 6・8 で作った部品と判断基準が
   そのまま使える
