// Package outbox, event store ile event bus arasındaki köprüdür:
// OUTBOX PATTERN.
//
// Sorun (Faz 2'de not edilen dual-write borcu): bir işlem hem event
// store'a yazıp hem bus'a yayınlarsa, ikisi arasında çökme "yazıldı ama
// yayınlanmadı" (veya tersi) bırakır — iki ayrı sistem, tek atomik
// işlem yok.
//
// Çözüm: yazma TEK yere gider (event store). Ayrı bir RELAY, store'u
// kalıcı bir checkpoint'ten takip eder ve bus'a yayınlar. Artık
// "yazıldı ama yayınlanmadı" geçici bir durumdur — relay yetişir.
//
// Teslim garantisi AT-LEAST-ONCE: checkpoint parti (batch) sonunda
// ilerletilir, yani parti ortasında çökme o partiyi yeniden yayınlar.
// Bunu iki katman karşılar: (1) yayın tarafında Msg-Id dedupe penceresi
// (event'in global sırası), (2) tüketici tarafında idempotentlik
// (global sıra ile dedupe). Exactly-once teslim yoktur; idempotentlik
// vardır.
package outbox

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"shardlands/pkg/bus"
	"shardlands/pkg/es"
	"shardlands/pkg/storage"
)

// Envelope, bus üzerinde taşınan event'in tel biçimidir.
type Envelope struct {
	Global uint64 `json:"g"`
	Stream string `json:"s"`
	Seq    uint64 `json:"q"`
	Type   string `json:"t"`
	Data   []byte `json:"d"`
	At     int64  `json:"at"`
}

// ToEvent, envelope'u es.Event'e çevirir (tüketiciler için).
func (e Envelope) ToEvent() es.Event {
	return es.Event{
		Global: e.Global, Stream: e.Stream, Seq: e.Seq,
		Type: e.Type, Data: e.Data, At: e.At,
	}
}

// Decode, bus mesajından envelope çıkarır.
func Decode(data []byte) (Envelope, error) {
	var env Envelope
	err := json.Unmarshal(data, &env)
	return env, err
}

var checkpointKey = []byte("outbox/checkpoint")

// Relay, store → bus köprüsü.
type Relay struct {
	store *es.Store
	b     bus.Bus
	db    *storage.DB // checkpoint'in kalıcı yeri
	batch int

	checkpoint atomic.Uint64
	published  atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// Open, checkpoint'i dir altında saklayan bir relay kurar (başlatmaz).
func Open(store *es.Store, b bus.Bus, dir string) (*Relay, error) {
	db, err := storage.Open(dir, storage.Options{})
	if err != nil {
		return nil, err
	}
	r := &Relay{
		store: store, b: b, db: db, batch: 256,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	val, err := db.Get(checkpointKey)
	switch {
	case err == nil:
		if len(val) == 8 {
			r.checkpoint.Store(binary.BigEndian.Uint64(val))
		}
	case errors.Is(err, storage.ErrNotFound):
		// ilk çalıştırma: 0'dan başla
	default:
		db.Close()
		return nil, err
	}
	return r, nil
}

// Start, relay döngüsünü başlatır.
func (r *Relay) Start() { go r.run() }

// Checkpoint, yayınlanmış son event'in global sırası.
func (r *Relay) Checkpoint() uint64 { return r.checkpoint.Load() }

// Published, bu süreçte yayınlanan mesaj sayısı (gözlem).
func (r *Relay) Published() int64 { return r.published.Load() }

func (r *Relay) run() {
	defer close(r.done)
	notify, cancel := r.store.Subscribe()
	defer cancel()

	for {
		evs, err := r.store.ReadAll(r.checkpoint.Load()+1, r.batch)
		if err != nil {
			log.Printf("outbox: read: %v", err)
		}
		if len(evs) > 0 {
			if err := r.publishBatch(evs); err != nil {
				log.Printf("outbox: publish: %v", err)
				// Checkpoint İLERLEMEDİ: aynı parti yeniden denenecek.
				select {
				case <-time.After(200 * time.Millisecond):
				case <-r.stop:
					return
				}
				continue
			}
			continue // devamı olabilir
		}
		select {
		case <-notify:
		case <-r.stop:
			return
		case <-time.After(time.Second): // güvenlik ağı (kaçan sinyal)
		}
	}
}

// publishBatch, partiyi sırayla yayınlar ve SONUNDA checkpoint'i
// kalıcılaştırır. Parti ortasında çökme partiyi tekrarlatır
// (at-least-once) — dedupe ve idempotentlik bunu karşılar.
func (r *Relay) publishBatch(evs []es.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, e := range evs {
		data, err := json.Marshal(Envelope{
			Global: e.Global, Stream: e.Stream, Seq: e.Seq,
			Type: e.Type, Data: e.Data, At: e.At,
		})
		if err != nil {
			return err
		}
		// Msg-Id = global sıra: yayın tarafı dedupe anahtarı.
		id := strconv.FormatUint(e.Global, 10)
		if err := r.b.Publish(ctx, bus.EventSubject(e.Type), id, data); err != nil {
			return err
		}
		r.published.Add(1)
	}
	last := evs[len(evs)-1].Global
	if err := r.saveCheckpoint(last); err != nil {
		return err
	}
	r.checkpoint.Store(last)
	return nil
}

func (r *Relay) saveCheckpoint(v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return r.db.Put(checkpointKey, buf[:])
}

// WaitCaughtUp, relay'in store'un sonuna yetişmesini bekler (testler ve
// kontrollü kapanış için).
func (r *Relay) WaitCaughtUp(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.checkpoint.Load() >= r.store.LastGlobal() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (r *Relay) Close() error {
	close(r.stop)
	<-r.done
	return r.db.Close()
}
