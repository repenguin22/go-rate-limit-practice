# Stage 6: HTTPミドルウェア

実装: [httplimit.go](../middleware/httplimit.go) / テスト: [httplimit_test.go](../middleware/httplimit_test.go) / デモ: [main.go](../cmd/demo/main.go)

## 1. アルゴリズムを「使える形」にする

ここまで作った `Limiter` はただの判定器で、単体ではサービスを守れない。
実際のHTTPサーバーに組み込むには、アルゴリズム以外の決めごとが必要になる:

1. **誰ごとに制限するか**(key の選び方)
2. **拒否をどう伝えるか**(ステータスコードとヘッダ)
3. **リミッター自体が壊れたらどうするか**(fail-open / fail-close)

この3つはアルゴリズムから独立した**運用上の判断**なので、ミドルウェアという
別レイヤーに切り出す。Go では `func(http.Handler) http.Handler` の形に包むのが
定石で、本実装では設定を持てるよう構造体 + `Wrap` メソッドにしている:

```go
mw, _ := middleware.New(limiter, middleware.KeyByIP)
srv := &http.Server{Handler: mw.Wrap(mux)}
```

`Limiter` インターフェースの設計(Stage 0)がここで効いてくる。ミドルウェアは
どのアルゴリズムか知らないし、インメモリか Redis かも知らない。

## 2. 決めごと① key の選び方 —— KeyFunc

「誰ごとに」はアプリケーションの決定なので、関数として注入する:

```go
type KeyFunc func(*http.Request) (string, error)
```

### KeyByIP と、その落とし穴

匿名トラフィックの制限は接続元IPが基本。ただしIPを key にするのは
見た目より難しく、セキュリティ上の罠が多い:

- **`X-Forwarded-For` を信じてはいけない**。クライアントが自由に書ける
  ヘッダなので、これを key にすると攻撃者はリクエストごとに偽IPを名乗って
  制限を無限にすり抜けられる。`KeyByIP` が `r.RemoteAddr`(実際のTCP接続の
  相手)しか見ないのは意図的な設計
- **ではプロキシ/LB配下では?** `RemoteAddr` は全部LBのIPになってしまう。
  正解は「**信頼できるプロキシの数だけ** XFF を右から遡った値を使う」。
  これはインフラ構成に依存するので、汎用ライブラリはデフォルトで
  XFF を見ない・使う側が明示的に設定する、が正しい態度
- **共有IPの巻き添え**。企業NATやCGNATでは数千人が1つのIPを共有する。
  IPだけを key にすると1人の暴走で全員が巻き添えになる。認証済みなら
  ユーザーIDを使うべき理由のひとつ

### KeyByHeader と「key が取れないとき」

APIキーヘッダを key にする `KeyByHeader("X-API-Key")` も用意した。
重要なのは**key が特定できないリクエストの扱い**:

```go
key, err := m.key(r)
if err != nil {
	http.Error(w, "cannot determine rate limit key", http.StatusBadRequest)
	return
}
```

「key 不明なら制限なしで通す」にすると、ヘッダを付けないだけで制限を
素通りできる抜け道になる。**識別できないものは拒否**が安全側の設計。
`TestWrap_KeyErrorRejectsWith400` では「リミッターに問い合わせすら
しない」ことまで検証している。

## 3. 決めごと② 拒否の伝え方 —— 429 とヘッダ

### ステータスコード

- 拒否 = **429 Too Many Requests**(RFC 6585)。「後で再試行すれば通る」の意味
- リミッター障害で fail-close = **503 Service Unavailable**。クライアントの
  せいではないので 4xx にしない

### ヘッダ

```
X-RateLimit-Limit: 10        ← 上限
X-RateLimit-Remaining: 7     ← 残り回数
X-RateLimit-Reset: 1767225660 ← リセット時刻(Unix秒)
Retry-After: 3               ← (429時のみ)何秒後に再試行してよいか
```

`Result` に判定以外の情報を持たせた(Stage 0)のはこのため。行儀のよい
クライアントは `Remaining` を見て自主的にペースを落とし、429 を受けたら
`Retry-After` だけ待つ。**ヘッダは「拒否」を「協調」に変える仕組み**。

細かいが実務で効く3点:

- **Retry-After は切り上げる**。2.5秒を「2」と伝えると、案内どおり待った
  クライアントがまだ拒否される。`TestWrap_DeniedRequestGets429` で
  2.5秒→"3" を検証している
- **Go はヘッダ名を正規化する**。`Header().Set("X-RateLimit-Limit", …)` は
  ワイヤ上では `X-Ratelimit-Limit` になる(ハイフン区切りの先頭だけ大文字化)。
  HTTP のヘッダ名は大文字小文字を区別しない(RFC 9110。HTTP/2 に至っては
  全部小文字で送る)ので実害はないが、curl で見たとき驚かないように
- **`X-RateLimit-Limit` の意味はアルゴリズム依存**。ウィンドウ系では
  「window あたりの回数」だが、バケット系(Token / Leaky)の `Result.Limit` は
  容量(burst)であって補充レートではない。APIドキュメントとして公開する
  ときは、どちらの意味かを明記すること

なお IETF でヘッダの標準化が進行中(`RateLimit-Policy` / `RateLimit`)だが
まだドラフトなので、広く通用する `X-RateLimit-*` 慣習形式を使った。

## 4. 決めごと③ リミッターが壊れたら —— fail-open / fail-close

インメモリ実装はまず失敗しないが、Stage 8 の Redis 版は**普通に落ちる**
(ネットワーク断、Redis再起動、タイムアウト)。そのとき2つの選択肢がある:

| | FailOpen(デフォルト) | FailClose |
|---|---|---|
| 挙動 | 制限なしで通す | 503 で拒否 |
| 優先するもの | 可用性 | 保護 |
| 向いている用途 | 一般APIの流量制限 | ログイン試行制限、決済など |
| リスク | 障害中は無制限になる | リミッター障害 = 全断 |

考え方: レートリミットが「あったほうがよい保護」なら fail-open
(リミッターの障害でサービスまで道連れにしない)。「ないと危険な防御」
なら fail-close(ブルートフォース対策が外れた状態で通すくらいなら止める)。
これは**セキュリティ設計の一般原則**で、認可・WAF などでも同じ判断が出てくる。

どちらでもエラーは必ずログに残す(`slog`)。fail-open で黙って通し続けると
「リミッターが1週間死んでいたことに誰も気づかない」が起きる。本来は
メトリクス(エラー率アラート)も付けるべきところ。

## 5. デモサーバーで体感する

```sh
go run ./cmd/demo -algo token -limit 5 -window 10s
# 別ターミナルで:
for i in $(seq 1 8); do curl -s -i localhost:8080/ | head -n 6; echo; done
```

5回目までは 200、6回目から 429 + `Retry-After: 2`(10s/5個 = 2秒に1個補充)
が返る。`-algo fixed` に変えて同じことをすると、`Retry-After` が
「ウィンドウ末尾までの残り秒数」になる違いが観察できる。
`-algo leaky -burst 1` なら完全平滑化(2秒に1回しか通らない)を体感できる。

デモ本体([main.go](../cmd/demo/main.go))の読みどころ:

- `-algo` の switch でリミッターを差し替えても、ミドルウェアもハンドラーも
  一切変わらない(インターフェースの効能)
- `signal.NotifyContext` + `srv.Shutdown` によるグレースフルシャットダウン。
  Ctrl+C で処理中のリクエストを完遂してから終了する
- `ReadHeaderTimeout` の設定(Slowloris 攻撃対策。レートリミットと同様
  「サーバーを守る」系の設定として必須)

## 6. 演習(理解の確認)

1. デモを `-algo fixed -limit 5 -window 10s` で起動し、429 の `Retry-After` が
   トークンバケットとどう違うか観察する(ウィンドウ境界を待つ vs 補充を待つ)
2. 「認証済みユーザーはユーザーIDで 100回/分、未認証はIPで 10回/分」を
   実現する構成を考えてみる(ヒント: KeyFunc の合成だけでは足りない。
   リミッターも2つ必要になる)
3. `X-RateLimit-Remaining` を返すことにはデメリットもある。攻撃者にとって
   どんな情報になるか考えてみる(ヒント: 制限値の探索が不要になる)

## 7. 次のステージ

[Stage 7: x/time/rate を読む](08-x-time-rate.md) — 準標準ライブラリの
トークンバケット実装を読み、自作実装と比較する。`Wait`(ブロック式)や
`Reserve`(予約式)など、`Allow` 以外のAPI設計も学ぶ。
