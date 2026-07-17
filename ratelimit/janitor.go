package ratelimit

import "time"

// runJanitor は stop が閉じられるまで interval ごとに sweep を呼び続ける。
// 各リミッターのバックグラウンド掃除ゴルーチンの共通実装。
func runJanitor(stop <-chan struct{}, interval time.Duration, sweep func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			sweep()
		}
	}
}
