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
	"math"
	"sort"
	"strings"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/services/inventory"
)

const (
	Width    = 800.0 // dünya sınırları (px)
	Height   = 600.0
	Speed    = 200.0 // px/s
	TickRate = 20    // Tick/s — dt = 1/TickRate

	maxChatLen  = 120
	bubbleTicks = 4 * TickRate // balon ~4 saniye görünür

	GatherRadius = 48.0          // toplama menzili (px)
	RespawnTicks = 10 * TickRate // tüketilen node ~10 saniyede yeniden doğar
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

// ChatSaid, EventChatSaid event'inin veri şeklidir. (Envanter/takas
// event'leri services/inventory ve services/trade'de tanımlıdır; hareket
// bilerek event değildir.)
type ChatSaid struct {
	PlayerID string `json:"playerId"`
	Name     string `json:"name"`
	Text     string `json:"text"`
}

// nodeLayout: hub'ın sabit kaynak yerleşimi. Deterministik sıra hem
// testler hem snapshot kararlılığı için (map değil slice).
var nodeLayout = []NodeState{
	{ID: "n1", Kind: "wood", X: 150, Y: 150},
	{ID: "n2", Kind: "wood", X: 650, Y: 150},
	{ID: "n3", Kind: "crystal", X: 150, Y: 450},
	{ID: "n4", Kind: "crystal", X: 650, Y: 450},
	{ID: "n5", Kind: "wood", X: 400, Y: 180},
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

// Gather: oyuncu toplamak istiyor. Hangi node'un toplanacağına SUNUCU
// karar verir (menzildeki en yakın müsait node) — istemciye node
// seçtirmek, menzil hilesine kapı açardı.
type Gather struct {
	PlayerID string
}

// Tick: simülasyonu bir adım ilerlet (dış zamanlayıcıdan).
type Tick struct{}

// ---- oturumlara yayınlanan mesaj ----

// Snapshot, tick sonrası dünyanın tamamıdır (oyuncular ID'ye göre,
// node'lar yerleşim sırasına göre — deterministik). Alıcılar dilimleri
// DEĞİŞTİRMEMELİDİR: aynı değer tüm oturumlara gönderilir.
type Snapshot struct {
	Tick    uint64
	Players []PlayerState
	Nodes   []NodeState
}

// NodeState, bir kaynak node'unun istemciye görünen halidir.
type NodeState struct {
	ID        string
	Kind      string
	X, Y      float64
	Available bool
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
		Name: "world",
		Producer: func() actor.Actor {
			h := &hub{players: map[string]*entity{}, events: events}
			for _, n := range nodeLayout {
				h.nodes = append(h.nodes, &node{NodeState: n})
			}
			return h
		},
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

type node struct {
	NodeState
	respawnAt uint64 // 0 = müsait; değilse bu tick'te yeniden doğar
}

func (n *node) available() bool { return n.respawnAt == 0 }

type hub struct {
	tick    uint64
	players map[string]*entity
	nodes   []*node
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
	case Gather:
		h.handleGather(m)
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

// handleGather: menzildeki en yakın müsait node'u tüket ve
// ResourceGathered event'ini oyuncunun envanter stream'ine yaz.
// Envanterin kendisi burada TUTULMAZ — sayımlar read model'in işi;
// dünya yalnızca node durumunu (müsait/tükenmiş) bilir.
func (h *hub) handleGather(m Gather) {
	e, ok := h.players[m.PlayerID]
	if !ok {
		return
	}
	var best *node
	bestDist := GatherRadius
	for _, n := range h.nodes {
		if !n.available() {
			continue
		}
		if d := math.Hypot(n.X-e.x, n.Y-e.y); d <= bestDist {
			best, bestDist = n, d
		}
	}
	if best == nil {
		return // menzilde müsait node yok
	}
	best.respawnAt = h.tick + RespawnTicks
	if h.events == nil {
		return
	}
	data, _ := json.Marshal(inventory.Gathered{
		PlayerID: e.id, Name: e.name, NodeID: best.ID, Kind: best.Kind, Amount: 1,
	})
	if _, err := h.events.Append(inventory.Stream(e.id), es.AnyVersion,
		es.EventData{Type: inventory.EventGathered, Data: data}); err != nil {
		log.Printf("world: gather event append: %v", err)
	}
}

func (h *hub) step() {
	const dt = 1.0 / TickRate
	for _, n := range h.nodes {
		if n.respawnAt != 0 && h.tick >= n.respawnAt {
			n.respawnAt = 0
		}
	}
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
	ns := make([]NodeState, len(h.nodes))
	for i, n := range h.nodes {
		ns[i] = n.NodeState
		ns[i].Available = n.available()
	}
	return Snapshot{Tick: h.tick, Players: ps, Nodes: ns}
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
