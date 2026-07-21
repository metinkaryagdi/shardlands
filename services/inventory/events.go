// Package inventory, oyuncu envanterinin hem event sözleşmesini (domain)
// hem READ MODEL'ini (sorgu tarafı) hem de komut tarafı işlemlerini
// (reserve/release/commit/receive) barındırır.
//
// Envanterin gerçeği inv-<player> stream'indeki event dizisidir. Bakiye
// iki bileşenlidir: Available (harcanabilir) ve Reserved (bir takas için
// tutulan). Rezervasyon, saga'ların "önce tut, sonra taahhüt/telafi et"
// desenini mümkün kılar — çifte harcamayı optimistic concurrency ile
// engeller (bkz. ops.go).
package inventory

import (
	"encoding/json"

	"shardlands/pkg/es"
)

const (
	EventGathered  = "ResourceGathered"
	EventReserved  = "ResourceReserved"
	EventReleased  = "ResourceReleased"  // telafi: rezervasyonu geri al
	EventCommitted = "ResourceCommitted" // rezerve mal kalıcı olarak çıktı
	EventReceived  = "ResourceReceived"  // karşı taraftan mal geldi
)

// Stream, oyuncunun envanter aggregate'inin stream adıdır.
func Stream(playerID string) string { return "inv-" + playerID }

// Gathered, EventGathered event'inin veri şeklidir.
type Gathered struct {
	PlayerID string `json:"playerId"`
	Name     string `json:"name"`
	NodeID   string `json:"nodeId"`
	Kind     string `json:"kind"`
	Amount   int    `json:"amount"`
}

// Move, takas kaynaklı bir bakiye hareketidir (reserve/release/commit/
// receive event'lerinin ortak veri şekli). TradeID hangi takasa ait
// olduğunu söyler — telafi ve denetim izi için.
type Move struct {
	PlayerID string `json:"playerId"`
	TradeID  string `json:"tradeId"`
	Kind     string `json:"kind"`
	Amount   int    `json:"amount"`
}

// Balance, bir oyuncunun tür başına bakiyesidir.
type Balance struct {
	Available map[string]int
	Reserved  map[string]int
}

func newBalance() Balance {
	return Balance{Available: map[string]int{}, Reserved: map[string]int{}}
}

// Fold, envanter event'lerini bakiyeye indirger. Komut tarafı (Reserve
// öncesi kontrol) ve testler bunu kullanır; read model aynı geçişleri
// artımlı uygular.
func Fold(events []es.Event) Balance {
	b := newBalance()
	for _, e := range events {
		applyTo(&b, e)
	}
	return b
}

func applyTo(b *Balance, e es.Event) {
	switch e.Type {
	case EventGathered:
		var g Gathered
		if json.Unmarshal(e.Data, &g) == nil {
			adj(b.Available, g.Kind, g.Amount)
		}
	case EventReserved:
		if m, ok := move(e); ok {
			adj(b.Available, m.Kind, -m.Amount)
			adj(b.Reserved, m.Kind, m.Amount)
		}
	case EventReleased:
		if m, ok := move(e); ok {
			adj(b.Reserved, m.Kind, -m.Amount)
			adj(b.Available, m.Kind, m.Amount)
		}
	case EventCommitted:
		if m, ok := move(e); ok {
			adj(b.Reserved, m.Kind, -m.Amount)
		}
	case EventReceived:
		if m, ok := move(e); ok {
			adj(b.Available, m.Kind, m.Amount)
		}
	}
}

// adj, sayacı günceller ve sıfıra düşen anahtarı siler — haritalar
// yalnızca gerçekten var olan (sıfır olmayan) bakiyeleri taşır.
func adj(m map[string]int, kind string, delta int) {
	m[kind] += delta
	if m[kind] == 0 {
		delete(m, kind)
	}
}

func move(e es.Event) (Move, bool) {
	var m Move
	return m, json.Unmarshal(e.Data, &m) == nil
}
