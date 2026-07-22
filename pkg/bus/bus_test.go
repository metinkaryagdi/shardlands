package bus_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"shardlands/pkg/bus"
)

func testBus(t *testing.T) bus.Bus {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-bus-*")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := bus.StartEmbedded(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := bus.Connect(srv.URL())
	if err != nil {
		srv.Shutdown()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
	})
	return b
}

func publish(t *testing.T, b bus.Bus, id, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Publish(ctx, bus.EventSubject("Test"), id, []byte(body)); err != nil {
		t.Fatal(err)
	}
}

// Temel: yayınlanan mesaj dayanıklı tüketiciye ulaşır.
func TestPublishSubscribe(t *testing.T) {
	b := testBus(t)
	got := make(chan string, 8)
	sub, err := b.Subscribe(bus.SubscribeOptions{Durable: "basic"}, func(_ context.Context, m bus.Message) error {
		got <- string(m.Data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()

	publish(t, b, "1", "merhaba")
	select {
	case s := <-got:
		if s != "merhaba" {
			t.Fatalf("got %q", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("message never delivered")
	}
}

// Handler hata dönerse mesaj YENİDEN teslim edilir; sonradan başarılı
// olunca ack'lenir (at-least-once + retry).
func TestRedeliveryUntilSuccess(t *testing.T) {
	b := testBus(t)
	var attempts atomic.Int32
	done := make(chan int, 4)

	sub, err := b.Subscribe(bus.SubscribeOptions{
		Durable:    "retry",
		MaxDeliver: 10,
		AckWait:    2 * time.Second,
		Backoff:    func(int) time.Duration { return 50 * time.Millisecond },
	}, func(_ context.Context, m bus.Message) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("geçici hata")
		}
		done <- m.Deliveries
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()

	publish(t, b, "r1", "tekrar")
	select {
	case deliveries := <-done:
		if deliveries < 3 {
			t.Fatalf("succeeded on delivery %d, want >= 3", deliveries)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("never succeeded (attempts=%d)", attempts.Load())
	}
}

// Zehirli mesaj: MaxDeliver tükenince DLQ'ya taşınır ve ANA akış
// tıkanmaz (sonraki mesaj işlenir).
func TestPoisonMessageGoesToDLQ(t *testing.T) {
	b := testBus(t)
	dlq := make(chan string, 4)
	dlqSub, err := b.DeadLetters("poison", func(_ context.Context, m bus.Message) error {
		dlq <- string(m.Data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dlqSub.Stop()

	good := make(chan string, 4)
	sub, err := b.Subscribe(bus.SubscribeOptions{
		Durable:    "poison",
		MaxDeliver: 3,
		AckWait:    time.Second,
		Backoff:    func(int) time.Duration { return 20 * time.Millisecond },
	}, func(_ context.Context, m bus.Message) error {
		if string(m.Data) == "zehir" {
			return errors.New("işlenemez")
		}
		good <- string(m.Data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()

	publish(t, b, "p1", "zehir")
	select {
	case d := <-dlq:
		if d != "zehir" {
			t.Fatalf("dlq got %q", d)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("poison message never reached DLQ")
	}

	// Akış tıkanmamalı: sonraki mesaj normal işlenmeli.
	publish(t, b, "p2", "iyi")
	select {
	case g := <-good:
		if g != "iyi" {
			t.Fatalf("got %q", g)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream blocked after poison message")
	}
}

// Dayanıklı tüketici: abonelik dursa bile mesajlar birikir ve aynı
// durable adıyla dönünce kaldığı yerden devam eder.
func TestDurableResumesAfterStop(t *testing.T) {
	b := testBus(t)
	first := make(chan string, 8)
	sub, err := b.Subscribe(bus.SubscribeOptions{Durable: "resume"}, func(_ context.Context, m bus.Message) error {
		first <- string(m.Data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	publish(t, b, "d1", "bir")
	select {
	case <-first:
	case <-time.After(10 * time.Second):
		t.Fatal("first message not delivered")
	}
	sub.Stop()

	// Abonelik yokken yayınla.
	publish(t, b, "d2", "iki")
	publish(t, b, "d3", "üç")

	second := make(chan string, 8)
	sub2, err := b.Subscribe(bus.SubscribeOptions{Durable: "resume"}, func(_ context.Context, m bus.Message) error {
		second <- string(m.Data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Stop()

	seen := map[string]bool{}
	deadline := time.Now().Add(15 * time.Second)
	for len(seen) < 2 && time.Now().Before(deadline) {
		select {
		case s := <-second:
			seen[s] = true
		case <-time.After(time.Second):
		}
	}
	if !seen["iki"] || !seen["üç"] {
		t.Fatalf("durable did not resume, saw %v", seen)
	}
}

// Yayın tarafı dedupe: aynı Msg-Id iki kez yayınlanırsa tüketici bir kez
// görür (outbox relay restart'ında tekrarları azaltır — garanti değil,
// bu yüzden tüketiciler yine idempotent olmalı).
func TestPublishDedupeByMsgID(t *testing.T) {
	b := testBus(t)
	var count atomic.Int32
	sub, err := b.Subscribe(bus.SubscribeOptions{Durable: "dedupe"}, func(_ context.Context, m bus.Message) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()

	publish(t, b, "same-id", "a")
	publish(t, b, "same-id", "a")
	publish(t, b, "same-id", "a")

	time.Sleep(2 * time.Second)
	if got := count.Load(); got != 1 {
		t.Fatalf("delivered %d times, want 1 (publish-side dedupe)", got)
	}
}

// Backpressure: MaxInFlight, aynı anda işlenmemiş mesaj sayısını
// sınırlar — yavaş tüketici bus'ı sınırsız biriktirmeye zorlamaz.
func TestMaxInFlightLimitsConcurrency(t *testing.T) {
	b := testBus(t)
	const limit = 4
	var inFlight, peak atomic.Int32
	var mu sync.Mutex
	release := make(chan struct{})

	sub, err := b.Subscribe(bus.SubscribeOptions{
		Durable:     "inflight",
		MaxInFlight: limit,
		AckWait:     30 * time.Second,
	}, func(_ context.Context, m bus.Message) error {
		cur := inFlight.Add(1)
		mu.Lock()
		if cur > peak.Load() {
			peak.Store(cur)
		}
		mu.Unlock()
		<-release
		inFlight.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()

	for i := 0; i < 20; i++ {
		publish(t, b, fmt.Sprintf("f%d", i), "x")
	}
	time.Sleep(2 * time.Second)
	got := peak.Load()
	close(release)

	if got > limit {
		t.Fatalf("peak in-flight = %d, want <= %d (backpressure)", got, limit)
	}
	if got == 0 {
		t.Fatal("no messages were delivered")
	}
}
