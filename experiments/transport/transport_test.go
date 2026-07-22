package transport

import (
	"testing"
	"time"
)

// Sağlıklı ağda iki taşıma da tüm kareleri düşük gecikmeyle teslim eder
// (temel çizgi: fark kayıptan doğar, taşımanın kendisinden değil).
func TestBaselineBothDeliverAllFrames(t *testing.T) {
	cfg := Config{Frames: 40, Interval: 15 * time.Millisecond, FrameSize: 200}

	ws, _, err := RunWebSocket(cfg, 0, 0)
	if err != nil {
		t.Fatalf("websocket: %v", err)
	}
	qc, _, err := RunQUIC(cfg, 0)
	if err != nil {
		t.Fatalf("quic: %v", err)
	}
	t.Log(ws)
	t.Log(qc)

	if ws.Received != cfg.Frames {
		t.Fatalf("ws received %d/%d", ws.Received, cfg.Frames)
	}
	if qc.Received != cfg.Frames {
		t.Fatalf("quic received %d/%d", qc.Received, cfg.Frames)
	}
	if ws.P99 > 100*time.Millisecond || qc.P99 > 100*time.Millisecond {
		t.Fatalf("baseline latency too high: ws p99=%v quic p99=%v", ws.P99, qc.P99)
	}
}

// ASIL DENEY — kayıplı ağda head-of-line blocking:
//
//	TCP/WebSocket: kayıp segment yeniden iletilene kadar ARKADAKİ kareler
//	  de teslim edilemez → gecikme kuyruğu şişer (yüksek p99/max), ama
//	  hiçbir kare KAYBOLMAZ.
//	QUIC datagram: kayıp kare kaybolur, kuyruk tıkanmaz → gecikme düşük
//	  kalır, karşılığında birkaç kare eksik gelir.
//
// Oyun için doğru takas ikincisidir: yeni kare eskisini zaten geçersiz
// kılar, o yüzden eskiyi beklemek zarardır.
func TestLossyNetworkHeadOfLineBlocking(t *testing.T) {
	cfg := Config{Frames: 60, Interval: 15 * time.Millisecond, FrameSize: 200}
	const stall = 150 * time.Millisecond

	ws, stalls, err := RunWebSocket(cfg, 8, stall)
	if err != nil {
		t.Fatalf("websocket: %v", err)
	}
	qc, drops, err := RunQUIC(cfg, 8)
	if err != nil {
		t.Fatalf("quic: %v", err)
	}
	t.Logf("%s  (gecikme uygulanan parça: %d)", ws, stalls)
	t.Logf("%s  (düşürülen paket: %d)", qc, drops)

	if stalls == 0 {
		t.Fatal("stall proxy never fired; experiment invalid")
	}
	if drops == 0 {
		t.Fatal("drop proxy never fired; experiment invalid")
	}

	// TCP hiçbir kareyi kaybetmemeli (güvenilir taşıma).
	if ws.Received != cfg.Frames {
		t.Fatalf("ws lost frames (%d/%d) — TCP should be reliable", ws.Received, cfg.Frames)
	}
	// QUIC datagram kayıp yaşamalı (güvenilmez taşıma) ama çoğunu almalı.
	if qc.Received >= cfg.Frames {
		t.Fatalf("quic received everything (%d) — drops not applied to frames?", qc.Received)
	}
	if qc.Received < cfg.Frames/2 {
		t.Fatalf("quic received too few frames: %d/%d", qc.Received, cfg.Frames)
	}

	// ASIL İDDİA: HOL yüzünden TCP'nin kuyruk gecikmesi QUIC'inkinden
	// belirgin biçimde yüksek.
	if ws.P99 <= qc.P99 {
		t.Fatalf("expected TCP p99 (%v) > QUIC p99 (%v) under loss (HOL blocking)", ws.P99, qc.P99)
	}
	if ws.Max < stall {
		t.Fatalf("ws max latency %v < injected stall %v — HOL not observed", ws.Max, stall)
	}
	// QUIC tarafında kuyruk şişmesi olmamalı: gecikme enjekte edilen
	// duraklamanın çok altında kalmalı.
	if qc.P99 > stall/2 {
		t.Fatalf("quic p99 %v too high; datagrams should not queue behind losses", qc.P99)
	}
}
