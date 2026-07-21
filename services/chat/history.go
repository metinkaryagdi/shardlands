// Package chat, sohbet geçmişinin READ MODEL'idir (CQRS'in sorgu
// tarafı). Yazı yolu (world aktörü ChatSaid event'i basar) ile okuma
// yolu (bu projection + HTTP endpoint) tamamen ayrıdır; ikisini
// yalnızca event log bağlar.
//
// Projection deseni: checkpoint'ten itibaren oku → uygula → sinyal
// bekle → tekrar. Read model her an sıfırdan yeniden kurulabilir
// (checkpoint=0'dan replay) — bu, event sourcing'in "durum türetilir,
// kaybolabilir" vaadinin somut hali. Süreç içi tek eksiği: göz açıp
// kapayana kadarlık gecikme (eventual consistency).
package chat

import (
	"encoding/json"
	"log"
	"sync"

	"shardlands/pkg/es"
	"shardlands/services/world"
)

const maxKeep = 100

// Message, read model'in sorgu şeklidir (event şeklinden bilinçli
// olarak ayrı: sorgu tarafı kendi ihtiyacına göre şekillenir).
type Message struct {
	PlayerID string `json:"playerId"`
	Name     string `json:"name"`
	Text     string `json:"text"`
	At       int64  `json:"at"` // unix ms (event zamanı)
}

type History struct {
	store *es.Store

	mu   sync.RWMutex
	msgs []Message

	stop chan struct{}
	done chan struct{}
}

// NewHistory, projection'ı başlatır (arka planda event akışını izler).
func NewHistory(store *es.Store) *History {
	h := &History{
		store: store,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go h.run()
	return h
}

func (h *History) run() {
	defer close(h.done)
	notify, cancel := h.store.Subscribe()
	defer cancel()

	var checkpoint uint64
	for {
		evs, err := h.store.ReadAll(checkpoint+1, 256)
		if err != nil {
			log.Printf("chat history: read: %v", err)
		}
		if len(evs) > 0 {
			h.apply(evs)
			checkpoint = evs[len(evs)-1].Global
			continue // aynı turda devamı olabilir
		}
		select {
		case <-notify:
		case <-h.stop:
			return
		}
	}
}

func (h *History) apply(evs []es.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range evs {
		// Log paylaşımlı: yalnızca kendi ilgilendiğimiz event'leri işle,
		// gerisini yok say — her projection kendi süzgecini taşır.
		if e.Stream != world.ChatStream || e.Type != world.EventChatSaid {
			continue
		}
		var said world.ChatSaid
		if err := json.Unmarshal(e.Data, &said); err != nil {
			log.Printf("chat history: bad event %d: %v", e.Global, err)
			continue
		}
		h.msgs = append(h.msgs, Message{
			PlayerID: said.PlayerID, Name: said.Name, Text: said.Text, At: e.At,
		})
		if len(h.msgs) > maxKeep {
			h.msgs = h.msgs[len(h.msgs)-maxKeep:]
		}
	}
}

// Recent, en yeni n mesajı (eski→yeni sırayla) döner.
func (h *History) Recent(n int) []Message {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if n <= 0 || n > len(h.msgs) {
		n = len(h.msgs)
	}
	return append([]Message(nil), h.msgs[len(h.msgs)-n:]...)
}

// Close, projection'ı durdurur ve bitmesini bekler.
func (h *History) Close() {
	close(h.stop)
	<-h.done
}
