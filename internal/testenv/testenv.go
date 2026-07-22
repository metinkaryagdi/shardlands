// Package testenv, Faz 4 sonrası read model testleri için ortak ortam
// kurar: event store + gömülü event bus + outbox relay.
//
// Neden gerçek bus? Read model'ler artık bus'tan tüketiyor; sahte bir
// kaynakla test etmek asıl yolu (relay → bus → dedupe → apply) atlardı.
// Bedeli test başına ~1 sn kurulum; kazancı gerçek teslim semantiğini
// (at-least-once, sıra, ack) sınamak.
package testenv

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"shardlands/pkg/bus"
	"shardlands/pkg/es"
	"shardlands/services/outbox"
)

type Env struct {
	Dir   string
	Store *es.Store
	Bus   bus.Bus
	Relay *outbox.Relay

	srv *bus.Embedded
}

// New, izole bir ortam kurar ve testin sonunda temizler.
func New(t *testing.T) *Env {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-env-*")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := bus.StartEmbedded(filepath.Join(dir, "nats"))
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	b, err := bus.Connect(srv.URL())
	if err != nil {
		srv.Shutdown()
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	store, err := es.Open(filepath.Join(dir, "events"))
	if err != nil {
		b.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	relay, err := outbox.Open(store, b, filepath.Join(dir, "outbox"))
	if err != nil {
		store.Close()
		b.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	relay.Start()

	e := &Env{Dir: dir, Store: store, Bus: b, Relay: relay, srv: srv}
	t.Cleanup(func() {
		relay.Close()
		store.Close()
		b.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
	})
	return e
}

// Append, event store'a yazar (relay bus'a taşır).
func (e *Env) Append(t *testing.T, stream, typ string, data []byte) {
	t.Helper()
	if _, err := e.Store.Append(stream, es.AnyVersion, es.EventData{Type: typ, Data: data}); err != nil {
		t.Fatal(err)
	}
}

// WaitDelivered, relay'in store'un sonuna yetişmesini bekler (mesajların
// bus'a çıktığını garanti eder; tüketicinin işlemesi ayrıca beklenmeli).
func (e *Env) WaitDelivered(t *testing.T) {
	t.Helper()
	if !e.Relay.WaitCaughtUp(20 * time.Second) {
		t.Fatal("outbox relay did not catch up")
	}
}
