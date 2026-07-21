// Package es, pkg/storage (LSM) üzerinde append-only bir event store'dur.
//
// Event sourcing'in sözleşmesi: gerçeğin kaynağı mevcut durum değil,
// OLANLARIN SIRALI KAYDIDIR. Durum (read model'ler) event'lerden
// türetilir ve her zaman yeniden türetilebilir. Bu paketin verdiği
// garantiler:
//
//   - Global toplam sıra: her event'in benzersiz, kesin artan Global
//     numarası vardır (1'den). Read model'ler "checkpoint'ten itibaren
//     oku" ile ilerler.
//   - Stream'ler: her aggregate (oyuncu, takas, sohbet...) kendi
//     stream'inde kendi Seq'iyle yaşar; optimistic concurrency stream
//     versiyonu üzerinden çalışır ("sürüm N'e ekliyorum" — arada başka
//     yazan olduysa ErrVersionConflict).
//   - Atomik append: bir Append çağrısının TÜM event'leri tek storage
//     kaydına (batch) yazılır. Altta transaction yok; atomikliği "tek
//     anahtar = tek WAL kaydı" sağlar. Crash'te batch ya tamamen vardır
//     ya hiç yoktur.
//   - Değişmezlik = okuma tutarlılığı: event'ler asla güncellenmez;
//     bir okuyucunun gördüğü [1..checkpoint] aralığı sonsuza dek aynı
//     kalır. MVCC'nin event-store hali budur — versiyon, log
//     pozisyonudur. (Storage seviyesinde genel amaçlı snapshot
//     isolation hâlâ yok; Scan kilit tutar, bkz. pkg/storage.)
//
// Stream indeksi ve versiyonlar RAM'de tutulur ve açılışta log'un
// taranmasıyla YENİDEN KURULUR — tek doğruluk kaynağı log'un kendisidir
// (indeks bir cache'tir; LSM'deki manifest dersinin tersi: burada
// türetilebilir olduğu için persist edilmez).
package es

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"shardlands/pkg/storage"
)

var (
	// ErrVersionConflict: beklenen stream versiyonu tutmadı (arada
	// başka bir yazar ekleme yaptı). Çağıran güncel durumu okuyup
	// yeniden denemelidir.
	ErrVersionConflict = errors.New("es: stream version conflict")
)

// AnyVersion: versiyon kontrolü yapma (yarış önemli değilse).
const AnyVersion int64 = -1

// EventData, eklenmek istenen ham event'tir.
type EventData struct {
	Type string
	Data []byte
}

// Event, mağazadaki kayıtlı bir event'tir.
type Event struct {
	Global uint64 // mağaza genelinde toplam sıra (1 tabanlı)
	Stream string
	Seq    uint64 // stream içi sıra (1 tabanlı)
	Type   string
	Data   []byte
	At     int64 // unix milisaniye
}

// Store, tek yazarlı-çok okuyuculu event mağazasıdır (süreç içi).
type Store struct {
	db *storage.DB

	mu          sync.RWMutex
	global      uint64
	versions    map[string]uint64   // stream → son seq
	streamIdx   map[string][]uint64 // stream → event global'leri (seq sırasıyla)
	batchStarts []uint64            // her batch'in ilk global'i (artan)

	subMu   sync.Mutex
	subs    map[int]chan struct{}
	nextSub int
}

// Diskteki batch kaydı. Data []byte JSON'da base64'lenir.
type batchRecord struct {
	Stream      string       `json:"s"`
	At          int64        `json:"at"`
	FirstSeq    uint64       `json:"fs"`
	FirstGlobal uint64       `json:"fg"`
	Events      []batchEvent `json:"e"`
}

type batchEvent struct {
	Type string `json:"t"`
	Data []byte `json:"d"`
}

func gkey(global uint64) []byte {
	k := make([]byte, 2+8)
	copy(k, "g/")
	binary.BigEndian.PutUint64(k[2:], global)
	return k
}

// Open, dir altındaki mağazayı açar (yoksa yaratır) ve in-memory
// indeksi log'u tarayarak kurar.
func Open(dir string) (*Store, error) {
	db, err := storage.Open(dir, storage.Options{})
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:        db,
		versions:  map[string]uint64{},
		streamIdx: map[string][]uint64{},
		subs:      map[int]chan struct{}{},
	}
	err = db.Scan(func(k, v []byte) bool {
		if !bytes.HasPrefix(k, []byte("g/")) {
			err = fmt.Errorf("es: unexpected key %q in event store", k)
			return false
		}
		var b batchRecord
		if e := json.Unmarshal(v, &b); e != nil {
			err = fmt.Errorf("es: decode batch: %w", e)
			return false
		}
		if b.FirstGlobal != s.global+1 || b.FirstSeq != s.versions[b.Stream]+1 {
			err = fmt.Errorf("es: log gap at global %d (stream %s)", b.FirstGlobal, b.Stream)
			return false
		}
		s.batchStarts = append(s.batchStarts, b.FirstGlobal)
		for range b.Events {
			s.global++
			s.versions[b.Stream]++
			s.streamIdx[b.Stream] = append(s.streamIdx[b.Stream], s.global)
		}
		return true
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Append, event'leri stream'e atomik olarak ekler. expected >= 0 ise
// stream'in mevcut versiyonu tam olarak bu olmalıdır (optimistic
// concurrency); AnyVersion kontrolü atlar.
func (s *Store) Append(stream string, expected int64, events ...EventData) ([]Event, error) {
	if stream == "" {
		return nil, errors.New("es: stream name is required")
	}
	if len(events) == 0 {
		return nil, errors.New("es: at least one event is required")
	}
	s.mu.Lock()
	cur := s.versions[stream]
	if expected >= 0 && uint64(expected) != cur {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: stream %s at %d, expected %d", ErrVersionConflict, stream, cur, expected)
	}
	now := time.Now().UnixMilli()
	b := batchRecord{
		Stream:      stream,
		At:          now,
		FirstSeq:    cur + 1,
		FirstGlobal: s.global + 1,
		Events:      make([]batchEvent, len(events)),
	}
	out := make([]Event, len(events))
	for i, e := range events {
		b.Events[i] = batchEvent{Type: e.Type, Data: e.Data}
		out[i] = Event{
			Global: b.FirstGlobal + uint64(i),
			Stream: stream,
			Seq:    b.FirstSeq + uint64(i),
			Type:   e.Type,
			Data:   e.Data,
			At:     now,
		}
	}
	val, err := json.Marshal(b)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	// Tek Put = tek WAL kaydı = atomik batch. Hata olursa in-memory
	// durum DEĞİŞMEMİŞ olmalı; bu yüzden güncelleme Put'tan sonra.
	if err := s.db.Put(gkey(b.FirstGlobal), val); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.batchStarts = append(s.batchStarts, b.FirstGlobal)
	for range events {
		s.global++
		s.versions[stream]++
		s.streamIdx[stream] = append(s.streamIdx[stream], s.global)
	}
	s.mu.Unlock()

	s.notifyAll()
	return out, nil
}

// ReadAll, global sıradan (from dahil, 1 tabanlı; 0 = baştan) event
// akıtır. limit <= 0 ise sınırsız.
func (s *Store) ReadAll(from uint64, limit int) ([]Event, error) {
	if from == 0 {
		from = 1
	}
	s.mu.RLock()
	start := s.batchStartFor(from)
	s.mu.RUnlock()
	if start == 0 {
		return nil, nil // henüz hiç event yok
	}
	var out []Event
	var scanErr error
	err := s.db.ScanFrom(gkey(start), func(k, v []byte) bool {
		var b batchRecord
		if e := json.Unmarshal(v, &b); e != nil {
			scanErr = fmt.Errorf("es: decode batch: %w", e)
			return false
		}
		for i, be := range b.Events {
			g := b.FirstGlobal + uint64(i)
			if g < from {
				continue
			}
			out = append(out, Event{
				Global: g, Stream: b.Stream, Seq: b.FirstSeq + uint64(i),
				Type: be.Type, Data: be.Data, At: b.At,
			})
			if limit > 0 && len(out) >= limit {
				return false
			}
		}
		return true
	})
	if scanErr != nil {
		return nil, scanErr
	}
	return out, err
}

// ReadStream, stream'in fromSeq'ten (dahil, 1 tabanlı; 0 = baştan)
// itibaren event'lerini döner. limit <= 0 ise sınırsız.
func (s *Store) ReadStream(stream string, fromSeq uint64, limit int) ([]Event, error) {
	if fromSeq == 0 {
		fromSeq = 1
	}
	s.mu.RLock()
	idx := s.streamIdx[stream]
	if fromSeq > uint64(len(idx)) {
		s.mu.RUnlock()
		return nil, nil
	}
	globals := append([]uint64(nil), idx[fromSeq-1:]...)
	s.mu.RUnlock()
	if limit > 0 && len(globals) > limit {
		globals = globals[:limit]
	}

	out := make([]Event, 0, len(globals))
	var cached batchRecord
	var cachedStart uint64
	for i, g := range globals {
		s.mu.RLock()
		start := s.batchStartFor(g)
		s.mu.RUnlock()
		if start != cachedStart {
			val, err := s.db.Get(gkey(start))
			if err != nil {
				return nil, fmt.Errorf("es: batch %d: %w", start, err)
			}
			if err := json.Unmarshal(val, &cached); err != nil {
				return nil, fmt.Errorf("es: decode batch %d: %w", start, err)
			}
			cachedStart = start
		}
		off := g - cached.FirstGlobal
		out = append(out, Event{
			Global: g, Stream: stream, Seq: fromSeq + uint64(i),
			Type: cached.Events[off].Type, Data: cached.Events[off].Data, At: cached.At,
		})
	}
	return out, nil
}

// batchStartFor: global g'yi içeren batch'in ilk global'i (0 = yok).
// Çağıran en az RLock tutmalıdır.
func (s *Store) batchStartFor(g uint64) uint64 {
	if len(s.batchStarts) == 0 || g > s.global {
		if len(s.batchStarts) == 0 {
			return 0
		}
		// g mevcut son event'ten büyük olabilir; en son batch'ten
		// başlamak doğru (ScanFrom zaten filtreler).
	}
	i := sort.Search(len(s.batchStarts), func(i int) bool { return s.batchStarts[i] > g })
	if i == 0 {
		return s.batchStarts[0]
	}
	return s.batchStarts[i-1]
}

// Version, stream'in mevcut versiyonu (0 = hiç event yok).
func (s *Store) Version(stream string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[stream]
}

// LastGlobal, mağazadaki son event'in global sırası.
func (s *Store) LastGlobal() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.global
}

// Subscribe, her append'te sinyallenen coalesced bir kanal ve aboneliği
// kaldıran bir fonksiyon döner. Sinyal "yeni event var, checkpoint'ten
// oku" demektir — mailbox'lardaki notify deseniyle aynı sözleşme.
func (s *Store) Subscribe() (<-chan struct{}, func()) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	id := s.nextSub
	s.nextSub++
	ch := make(chan struct{}, 1)
	s.subs[id] = ch
	return ch, func() {
		s.subMu.Lock()
		defer s.subMu.Unlock()
		delete(s.subs, id)
	}
}

func (s *Store) notifyAll() {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, ch := range s.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (s *Store) Close() error { return s.db.Close() }
