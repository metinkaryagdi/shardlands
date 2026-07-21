// Package world, hub dünyasının simülasyonudur ve TEK BİR AKTÖRDÜR:
// tüm dünya durumuna yalnızca aktörün goroutine'i dokunur, kilit yok.
// Oturumlar (gateway'deki session aktörleri) dünyaya mesaj gönderir
// (Join/Leave/Input); dünya her Tick'te fiziği ilerletir ve herkese
// Snapshot yayınlar.
//
// Sunucu-otoriter model: istemci pozisyon DEĞİL niyet (basılı tuşlar)
// gönderir; pozisyonu yalnızca sunucu hesaplar. Hile ("ışınlanma")
// mimari olarak imkânsızlaşır. Bedeli: istemci kendi girdisinin
// sonucunu bir sonraki snapshot'ta görür (his gecikmesi); istemci
// tarafı tahmin (client-side prediction) bilinçli olarak Faz 5'e
// bırakıldı.
//
// Tick mesajı DIŞARIDAN gelir (cmd/server'daki zamanlayıcı): aktör
// zaman sahibi değildir. Bu, testlerin tick'i elle enjekte ederek
// simülasyonu deterministik sürmesini sağlar — Faz 0'daki "sinyalle
// senkronize test" dersinin devamı.
package world

import (
	"encoding/json"
	"log"
	"sort"
	"strings"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
)

const (
	Width    = 800.0 // dünya sınırları (px)
	Height   = 600.0
	Speed    = 200.0 // px/s
	TickRate = 20    // Tick/s — dt = 1/TickRate

	maxChatLen  = 120
	bubbleTicks = 4 * TickRate // balon ~4 saniye görünür
)

// Event sözleşmesi: read model'ler (projection'lar) bu sabitlere ve
// veri şekillerine bağlanır. Hareket BİLEREK event değil — 20Hz'lik
// geçici durum log'a yazılmaz; log kalıcı GERÇEKLER içindir (kim ne
// dedi, ne topladı, ne takas etti). "Her şeyi event yap" tuzağı,
// event sourcing'in en yaygın kötüye kullanımı.
const (
	ChatStream    = "chat"
	EventChatSaid = "ChatSaid"
)

// ChatSaid, EventChatSaid event'inin veri şeklidir.
type ChatSaid struct {
	PlayerID string `json:"playerId"`
	Name     string `json:"name"`
	Text     string `json:"text"`
}

// ---- aktöre gönderilen mesajlar ----

// Join: oturum dünyaya katılıyor; Session, Snapshot'ların gideceği Ref.
type Join struct {
	PlayerID string
	Name     string
	Session  *actor.Ref
}

// Leave: oyuncu ayrıldı (bilinmeyen id sessizce yok sayılır — bağlantı
// kopuşunda birden çok yoldan Leave gelebilir, idempotent olmalı).
type Leave struct {
	PlayerID string
}

// Input: basılı tuş durumu; bir sonraki durum değişikliğine kadar geçerli.
type Input struct {
	PlayerID              string
	Up, Down, Left, Right bool
}

// Chat: oyuncu bir şey söyledi. Komut aktörde doğrulanır; geçerliyse
// hem dünyada balon olur (geçici durum) hem event olarak kalıcılaşır.
type Chat struct {
	PlayerID string
	Text     string
}

// Tick: simülasyonu bir adım ilerlet (dış zamanlayıcıdan).
type Tick struct{}

// ---- oturumlara yayınlanan mesaj ----

// Snapshot, tick sonrası dünyanın tamamıdır (ID'ye göre sıralı —
// deterministik). Alıcılar dilimi DEĞİŞTİRMEMELİDİR: aynı değer tüm
// oturumlara gönderilir. Delta/AOI optimizasyonu Faz 5'te.
type Snapshot struct {
	Tick    uint64
	Players []PlayerState
}

type PlayerState struct {
	ID     string
	Name   string
	X, Y   float64
	Bubble string // aktif sohbet balonu ("" = yok)
}

// Props, hub aktörünün tanımını döner. events nil olabilir (testlerde
// kalıcılık istemeyen senaryolar için); nil ise event basılmaz.
func Props(events *es.Store) actor.Props {
	return actor.Props{
		Name:     "world",
		Producer: func() actor.Actor { return &hub{players: map[string]*entity{}, events: events} },
	}
}

type entity struct {
	id, name    string
	x, y        float64
	in          Input
	session     *actor.Ref
	bubble      string
	bubbleUntil uint64 // bu tick'te veya sonrasında balon silinir
}

type hub struct {
	tick    uint64
	players map[string]*entity
	events  *es.Store
}

func (h *hub) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case Join:
		h.players[m.PlayerID] = &entity{
			id: m.PlayerID, name: m.Name,
			x: Width / 2, y: Height / 2,
			session: m.Session,
		}
	case Leave:
		delete(h.players, m.PlayerID)
	case Input:
		if e, ok := h.players[m.PlayerID]; ok {
			e.in = m
		}
	case Chat:
		h.handleChat(m)
	case Tick:
		h.step()
		snap := h.snapshot()
		for _, e := range h.players {
			ctx.Send(e.session, snap)
		}
	}
}

// handleChat: komutu doğrula, geçerliyse balonu güncelle ve event bas.
// Dürüst not (dual-write): balon (dünya durumu) ve event log'u iki ayrı
// yazmadır; ideal ES'te durum, log'a abone bir projection olurdu.
// Süreç içi tek yazar olduğumuz için pratik risk düşük; outbox/bus
// çözümü Faz 4'ün konusu.
func (h *hub) handleChat(m Chat) {
	e, ok := h.players[m.PlayerID]
	if !ok {
		return
	}
	text := strings.TrimSpace(m.Text)
	if text == "" || len([]rune(text)) > maxChatLen {
		return
	}
	e.bubble = text
	e.bubbleUntil = h.tick + bubbleTicks
	if h.events == nil {
		return
	}
	data, _ := json.Marshal(ChatSaid{PlayerID: e.id, Name: e.name, Text: text})
	if _, err := h.events.Append(ChatStream, es.AnyVersion, es.EventData{Type: EventChatSaid, Data: data}); err != nil {
		log.Printf("world: chat event append: %v", err)
	}
}

func (h *hub) step() {
	const dt = 1.0 / TickRate
	for _, e := range h.players {
		if e.bubbleUntil != 0 && h.tick >= e.bubbleUntil {
			e.bubble, e.bubbleUntil = "", 0
		}
		var vx, vy float64
		if e.in.Left {
			vx -= Speed
		}
		if e.in.Right {
			vx += Speed
		}
		if e.in.Up {
			vy -= Speed
		}
		if e.in.Down {
			vy += Speed
		}
		e.x = clamp(e.x+vx*dt, 0, Width)
		e.y = clamp(e.y+vy*dt, 0, Height)
	}
	h.tick++
}

func (h *hub) snapshot() Snapshot {
	ps := make([]PlayerState, 0, len(h.players))
	for _, e := range h.players {
		ps = append(ps, PlayerState{ID: e.id, Name: e.name, X: e.x, Y: e.y, Bubble: e.bubble})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].ID < ps[j].ID })
	return Snapshot{Tick: h.tick, Players: ps}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
