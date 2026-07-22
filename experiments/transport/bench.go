package transport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"time"

	"github.com/gorilla/websocket"
	"github.com/quic-go/quic-go"
)

// Isınma payları: el sıkışma paketlerini bozmamak için ilk N parça/paket
// dokunulmadan geçirilir. Amaç bağlantı kurulumunu değil, KARE AKIŞINI
// ölçmek.
const (
	wsWarmupChunks    = 4
	quicWarmupPackets = 25
)

// Config, deney parametreleri.
type Config struct {
	Frames    int           // gönderilecek kare sayısı
	Interval  time.Duration // kareler arası süre (30Hz için ~33ms)
	FrameSize int           // kare gövdesi (bayt)
}

func (c *Config) withDefaults() {
	if c.Frames <= 0 {
		c.Frames = 90
	}
	if c.Interval <= 0 {
		c.Interval = 33 * time.Millisecond
	}
	if c.FrameSize <= 0 {
		c.FrameSize = 200
	}
}

// Stats, ölçüm sonucu.
type Stats struct {
	Transport string
	Sent      int
	Received  int
	P50       time.Duration
	P95       time.Duration
	P99       time.Duration
	Max       time.Duration
}

func (s Stats) String() string {
	loss := 0.0
	if s.Sent > 0 {
		loss = 100 * float64(s.Sent-s.Received) / float64(s.Sent)
	}
	return fmt.Sprintf("%-16s gönderilen=%d alınan=%d kayıp=%.1f%%  p50=%-8v p95=%-8v p99=%-8v max=%v",
		s.Transport, s.Sent, s.Received, loss, s.P50.Round(time.Microsecond),
		s.P95.Round(time.Microsecond), s.P99.Round(time.Microsecond), s.Max.Round(time.Microsecond))
}

// frame, kare gövdesi: [8B gönderim zamanı (unix ns)][dolgu].
func frame(size int, sentAt time.Time) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b, uint64(sentAt.UnixNano()))
	return b
}

func latencyOf(b []byte, now time.Time) (time.Duration, bool) {
	if len(b) < 8 {
		return 0, false
	}
	sent := time.Unix(0, int64(binary.BigEndian.Uint64(b)))
	return now.Sub(sent), true
}

func summarize(name string, sent int, lat []time.Duration) Stats {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pick := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		i := int(float64(len(lat)-1) * p)
		return lat[i]
	}
	s := Stats{Transport: name, Sent: sent, Received: len(lat),
		P50: pick(0.50), P95: pick(0.95), P99: pick(0.99)}
	if len(lat) > 0 {
		s.Max = lat[len(lat)-1]
	}
	return s
}

// ---- WebSocket (TCP) ----

// RunWebSocket, kareleri WS üzerinden akıtır ve gecikmeleri ölçer.
// stallEvery > 0 ise araya HOL etkisi üreten bir proxy konur.
func RunWebSocket(cfg Config, stallEvery int, stall time.Duration) (Stats, int64, error) {
	cfg.withDefaults()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		t := time.NewTicker(cfg.Interval)
		defer t.Stop()
		for i := 0; i < cfg.Frames; i++ {
			<-t.C
			if err := c.WriteMessage(websocket.BinaryMessage, frame(cfg.FrameSize, time.Now())); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	dialURL := "ws://" + srv.Listener.Addr().String()
	var proxy *StallProxy
	if stallEvery > 0 {
		p, err := NewStallProxy(srv.Listener.Addr().String(), stallEvery, stall, wsWarmupChunks)
		if err != nil {
			return Stats{}, 0, err
		}
		defer p.Close()
		proxy = p
		dialURL = "ws://" + p.Addr()
	}

	conn, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	if err != nil {
		return Stats{}, 0, err
	}
	defer conn.Close()

	lat := make([]time.Duration, 0, cfg.Frames)
	deadline := time.Now().Add(cfg.Interval*time.Duration(cfg.Frames) + 10*time.Second)
	for len(lat) < cfg.Frames && time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, b, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if d, ok := latencyOf(b, time.Now()); ok {
			lat = append(lat, d)
		}
	}
	name := "websocket(TCP)"
	var stalls int64
	if proxy != nil {
		name = "websocket(kayıplı)"
		// Sayaç RETURN'DEN ÖNCE okunmalı: defer içinde atamak isimsiz
		// dönüş değerini değiştirmez (dönüş değeri defer'lardan önce
		// hesaplanır).
		stalls = proxy.Stalls()
	}
	return summarize(name, cfg.Frames, lat), stalls, nil
}

// ---- QUIC (güvenilmez datagram) ----

func selfSignedTLS() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"shardlands-experiment"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"shardlands-bench"},
	}, nil
}

// RunQUIC, kareleri QUIC DATAGRAM'ları ile akıtır. dropEvery > 0 ise
// araya paketleri gerçekten düşüren bir UDP proxy konur.
func RunQUIC(cfg Config, dropEvery int) (Stats, int64, error) {
	cfg.withDefaults()
	tlsConf, err := selfSignedTLS()
	if err != nil {
		return Stats{}, 0, err
	}

	ln, err := quic.ListenAddr("127.0.0.1:0", tlsConf, &quic.Config{
		EnableDatagrams:      true,
		MaxIdleTimeout:       30 * time.Second,
		HandshakeIdleTimeout: 10 * time.Second,
	})
	if err != nil {
		return Stats{}, 0, err
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			return
		}
		t := time.NewTicker(cfg.Interval)
		defer t.Stop()
		for i := 0; i < cfg.Frames; i++ {
			<-t.C
			// SendDatagram: GÜVENİLMEZ — kayıp yeniden iletilmez,
			// dolayısıyla kuyruk tıkanmaz.
			if err := conn.SendDatagram(frame(cfg.FrameSize, time.Now())); err != nil {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
		conn.CloseWithError(0, "done")
	}()

	dialAddr := ln.Addr().String()
	var proxy *DropProxy
	if dropEvery > 0 {
		p, err := NewDropProxy(dialAddr, dropEvery, quicWarmupPackets)
		if err != nil {
			return Stats{}, 0, err
		}
		defer p.Close()
		proxy = p
		dialAddr = p.Addr()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, dialAddr, &tls.Config{
		InsecureSkipVerify: true, // deney: kendinden imzalı sertifika
		NextProtos:         []string{"shardlands-bench"},
	}, &quic.Config{
		EnableDatagrams:      true,
		MaxIdleTimeout:       30 * time.Second,
		HandshakeIdleTimeout: 10 * time.Second,
	})
	if err != nil {
		return Stats{}, 0, err
	}
	defer conn.CloseWithError(0, "bye")

	lat := make([]time.Duration, 0, cfg.Frames)
	deadline := time.Now().Add(cfg.Interval*time.Duration(cfg.Frames) + 10*time.Second)
	for len(lat) < cfg.Frames && time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		b, err := conn.ReceiveDatagram(rctx)
		rcancel()
		if err != nil {
			break
		}
		if d, ok := latencyOf(b, time.Now()); ok {
			lat = append(lat, d)
		}
	}
	name := "quic(datagram)"
	var drops int64
	if proxy != nil {
		name = "quic(kayıplı)"
		drops = proxy.Drops() // bkz. RunWebSocket: defer'de okumak yanlış
	}
	return summarize(name, cfg.Frames, lat), drops, nil
}
