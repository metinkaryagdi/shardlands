package outbox

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"shardlands/pkg/bus"
	"shardlands/pkg/es"
)

type fixture struct {
	store *es.Store
	b     bus.Bus
	dir   string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-outbox-*")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := bus.StartEmbedded(filepath.Join(dir, "nats"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := bus.Connect(srv.URL())
	if err != nil {
		srv.Shutdown()
		t.Fatal(err)
	}
	store, err := es.Open(filepath.Join(dir, "events"))
	if err != nil {
		b.Close()
		srv.Shutdown()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		b.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
	})
	return &fixture{store: store, b: b, dir: dir}
}

func (f *fixture) relay(t *testing.T) *Relay {
	t.Helper()
	r, err := Open(f.store, f.b, filepath.Join(f.dir, "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	return r
}

// collect, bus'tan gelen envelope'ları toplar.
func (f *fixture) collect(t *testing.T, durable string) (*sync.Mutex, *[]Envelope, bus.Subscription) {
	t.Helper()
	var mu sync.Mutex
	got := []Envelope{}
	sub, err := f.b.Subscribe(bus.SubscribeOptions{
		Name:        durable,
		Durable:     true,
		MaxInFlight: 1, // sıralı teslim (stream sırası korunur)
	}, func(_ context.Context, m bus.Message) error {
		env, err := Decode(m.Data)
		if err != nil {
			return err
		}
		mu.Lock()
		got = append(got, env)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return &mu, &got, sub
}

func appendEvent(t *testing.T, s *es.Store, stream, typ, body string) {
	t.Helper()
	if _, err := s.Append(stream, es.AnyVersion, es.EventData{Type: typ, Data: []byte(body)}); err != nil {
		t.Fatal(err)
	}
}

func waitCount(t *testing.T, mu *sync.Mutex, got *[]Envelope, want int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(*got)
		mu.Unlock()
		if n >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	n := len(*got)
	mu.Unlock()
	t.Fatalf("received %d envelopes, want %d", n, want)
}

// Store'a yazılan event'ler bus'a yayınlanır; içerik ve global sıra korunur.
func TestRelayPublishesStoreEvents(t *testing.T) {
	f := newFixture(t)
	mu, got, sub := f.collect(t, "relaytest")
	defer sub.Stop()

	r := f.relay(t)
	defer r.Close()

	appendEvent(t, f.store, "chat", "ChatSaid", `{"text":"bir"}`)
	appendEvent(t, f.store, "inv-p1", "ResourceGathered", `{"kind":"wood"}`)

	waitCount(t, mu, got, 2, 20*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if (*got)[0].Global != 1 || (*got)[0].Type != "ChatSaid" || (*got)[0].Stream != "chat" {
		t.Fatalf("first envelope = %+v", (*got)[0])
	}
	if (*got)[1].Global != 2 || (*got)[1].Type != "ResourceGathered" {
		t.Fatalf("second envelope = %+v", (*got)[1])
	}
	if string((*got)[0].Data) != `{"text":"bir"}` {
		t.Fatalf("payload lost: %s", (*got)[0].Data)
	}
}

// Sıra korunur: N event global sıraya göre teslim edilir (tek akış,
// MaxInFlight=1).
func TestRelayPreservesOrder(t *testing.T) {
	f := newFixture(t)
	mu, got, sub := f.collect(t, "ordertest")
	defer sub.Stop()

	r := f.relay(t)
	defer r.Close()

	const n = 25
	for i := 0; i < n; i++ {
		appendEvent(t, f.store, "chat", "ChatSaid", fmt.Sprintf(`{"i":%d}`, i))
	}
	waitCount(t, mu, got, n, 30*time.Second)

	mu.Lock()
	defer mu.Unlock()
	for i, env := range (*got)[:n] {
		if env.Global != uint64(i+1) {
			t.Fatalf("envelope %d has global %d, want %d (order broken)", i, env.Global, i+1)
		}
	}
}

// Checkpoint kalıcı: relay durup yeniden açılınca baştan yayınlamaz,
// yalnız yeni event'leri gönderir.
func TestCheckpointSurvivesRestart(t *testing.T) {
	f := newFixture(t)

	r := f.relay(t)
	appendEvent(t, f.store, "chat", "ChatSaid", `{"i":1}`)
	appendEvent(t, f.store, "chat", "ChatSaid", `{"i":2}`)
	if !r.WaitCaughtUp(20 * time.Second) {
		t.Fatal("relay did not catch up")
	}
	cp := r.Checkpoint()
	if cp != 2 {
		t.Fatalf("checkpoint = %d, want 2", cp)
	}
	published := r.Published()
	r.Close()

	// Yeni relay: checkpoint diskten okunmalı.
	r2, err := Open(f.store, f.b, filepath.Join(f.dir, "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if r2.Checkpoint() != cp {
		t.Fatalf("reopened checkpoint = %d, want %d", r2.Checkpoint(), cp)
	}
	r2.Start()
	defer r2.Close()

	appendEvent(t, f.store, "chat", "ChatSaid", `{"i":3}`)
	if !r2.WaitCaughtUp(20 * time.Second) {
		t.Fatal("relay2 did not catch up")
	}
	// Yalnız 1 yeni mesaj yayınlanmış olmalı (eskiler tekrar edilmedi).
	if got := r2.Published(); got != 1 {
		t.Fatalf("republished %d messages after restart, want 1 (checkpoint honored, before=%d)", got, published)
	}
}

// Relay çalışmıyorken yazılan event'ler, relay dönünce yetişir
// (store tek doğruluk kaynağı; bus'a yayın gecikmeli olabilir).
func TestRelayCatchesUpAfterDowntime(t *testing.T) {
	f := newFixture(t)
	mu, got, sub := f.collect(t, "catchup")
	defer sub.Stop()

	// Relay YOKKEN yaz.
	for i := 0; i < 5; i++ {
		appendEvent(t, f.store, "chat", "ChatSaid", fmt.Sprintf(`{"i":%d}`, i))
	}
	r := f.relay(t)
	defer r.Close()

	waitCount(t, mu, got, 5, 20*time.Second)
	if !r.WaitCaughtUp(10 * time.Second) {
		t.Fatal("relay did not catch up")
	}
}
