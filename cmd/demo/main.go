// Command demo はレートリミットを体感するためのデモHTTPサーバー。
//
// 使い方:
//
//	go run ./cmd/demo -algo token -limit 5 -window 10s
//	for i in $(seq 1 8); do curl -i -s localhost:8080/ | head -n 6; done
//
// アルゴリズムを切り替えても呼び出し側(このファイル)のコードが
// 変わらないことが Limiter インターフェースの効能。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/repenguin22/go-rate-limit-practice/middleware"
	"github.com/repenguin22/go-rate-limit-practice/ratelimit"
)

func main() {
	var (
		addr   = flag.String("addr", ":8080", "listen address")
		algo   = flag.String("algo", "token", "algorithm: fixed | swlog | swcounter | token | leaky")
		limit  = flag.Int("limit", 5, "requests allowed per window")
		window = flag.Duration("window", 10*time.Second, "window / refill period")
		burst  = flag.Int("burst", 0, "bucket capacity for token/leaky (0 = same as limit)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	limiter, closer, err := newLimiter(*algo, *limit, *window, *burst)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	defer closer() //nolint:errcheck

	mw, err := middleware.New(limiter, middleware.KeyByIP, middleware.WithLogger(logger))
	if err != nil {
		logger.Error("middleware setup failed", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"message": "hello",
			"time":    time.Now().Format(time.RFC3339Nano),
		})
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mw.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Ctrl+C / SIGTERM でグレースフルに終了する。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx) //nolint:errcheck
	}()

	logger.Info("demo server listening",
		"addr", *addr, "algo", *algo, "limit", *limit, "window", *window)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
	logger.Info("shut down cleanly")
}

// newLimiter は名前からリミッターを組み立てる。戻り値の closer は
// ジャニター停止用(io.Closer にせず func にしているのは呼びやすさのため)。
func newLimiter(algo string, limit int, window time.Duration, burst int) (ratelimit.Limiter, func() error, error) {
	var opts []ratelimit.Option
	if burst > 0 {
		opts = append(opts, ratelimit.WithBurst(burst))
	}

	switch algo {
	case "fixed":
		l, err := ratelimit.NewFixedWindow(limit, window, opts...)
		return l, closerOf(l, err), err
	case "swlog":
		l, err := ratelimit.NewSlidingWindowLog(limit, window, opts...)
		return l, closerOf(l, err), err
	case "swcounter":
		l, err := ratelimit.NewSlidingWindowCounter(limit, window, opts...)
		return l, closerOf(l, err), err
	case "token":
		l, err := ratelimit.NewTokenBucket(limit, window, opts...)
		return l, closerOf(l, err), err
	case "leaky":
		l, err := ratelimit.NewLeakyBucket(limit, window, opts...)
		return l, closerOf(l, err), err
	default:
		return nil, nil, fmt.Errorf("unknown algorithm %q", algo)
	}
}

func closerOf(c interface{ Close() error }, err error) func() error {
	if err != nil {
		return func() error { return nil }
	}
	return c.Close
}
