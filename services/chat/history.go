// Package chat, sohbet geçmişinin READ MODEL'idir (CQRS'in sorgu
// tarafı). Yazı yolu (world aktörü ChatSaid event'i basar) ile okuma
// yolu (bu projection + HTTP endpoint) tamamen ayrıdır.
//
// Faz 4'te akış kaynağı DEĞİŞTİ: artık event store'a doğrudan abone
// değil, EVENT BUS'tan tüketiyor (outbox relay store'u bus'a taşıyor).
// Kazanç: üretici ile tüketici ayrıştı — bu read model başka bir
// süreçte/serviste çalışabilir. Bedeli: at-least-once teslim, bu yüzden
// uygulama idempotent olmalı (outbox.Consume global sıra ile dedupe
// eder).
package chat

import (
	"encoding/json"
	"log"
	"sync"

	"shardlands/pkg/bus"
	"shardlands/pkg/es"
	"shardlands/services/outbox"
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
	mu   sync.RWMutex
	msgs []Message

	sub bus.Subscription
}

// NewHistory, bus'tan tüketen projection'ı başlatır.
func NewHistory(b bus.Bus) (*History, error) {
	h := &History{}
	sub, err := outbox.Consume(b, "chat-history", h.apply)
	if err != nil {
		return nil, err
	}
	h.sub = sub
	return h, nil
}

func (h *History) apply(e es.Event) error {
	// Log paylaşımlı: yalnızca ilgilendiğimiz event'i işle.
	if e.Stream != world.ChatStream || e.Type != world.EventChatSaid {
		return nil
	}
	var said world.ChatSaid
	if err := json.Unmarshal(e.Data, &said); err != nil {
		// Bozuk veri kalıcı hatadır; yeniden denemek düzeltmez.
		log.Printf("chat history: bad event %d: %v", e.Global, err)
		return nil
	}
	h.mu.Lock()
	h.msgs = append(h.msgs, Message{
		PlayerID: said.PlayerID, Name: said.Name, Text: said.Text, At: e.At,
	})
	if len(h.msgs) > maxKeep {
		h.msgs = h.msgs[len(h.msgs)-maxKeep:]
	}
	h.mu.Unlock()
	return nil
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

// Close, projection'ı durdurur.
func (h *History) Close() {
	if h.sub != nil {
		h.sub.Stop()
	}
}
