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
	"sort"

	"shardlands/pkg/actor"
)

const (
	Width    = 800.0 // dünya sınırları (px)
	Height   = 600.0
	Speed    = 200.0 // px/s
	TickRate = 20    // Tick/s — dt = 1/TickRate
)

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
	ID   string
	Name string
	X, Y float64
}

// Props, hub aktörünün tanımını döner.
func Props() actor.Props {
	return actor.Props{
		Name:     "world",
		Producer: func() actor.Actor { return &hub{players: map[string]*entity{}} },
	}
}

type entity struct {
	id, name string
	x, y     float64
	in       Input
	session  *actor.Ref
}

type hub struct {
	tick    uint64
	players map[string]*entity
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
	case Tick:
		h.step()
		snap := h.snapshot()
		for _, e := range h.players {
			ctx.Send(e.session, snap)
		}
	}
}

func (h *hub) step() {
	const dt = 1.0 / TickRate
	for _, e := range h.players {
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
		ps = append(ps, PlayerState{ID: e.id, Name: e.name, X: e.x, Y: e.y})
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
