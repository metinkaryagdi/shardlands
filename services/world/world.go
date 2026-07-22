// Package world, hub dünyasının simülasyonudur. Faz 3'ten itibaren dünya
// BÖLGELERE (grid hücreleri) ayrılmıştır ve her bölge KENDİ AKTÖRÜDÜR
// (region.go): bir bölgenin durumuna yalnız o aktörün goroutine'i
// dokunur. Consistent hashing bölge→shard node eşlemesi yapar
// (router.go); mekânsal yerellik (aynı bölgedekiler birbirini görür) ile
// esnek node ataması bir arada.
//
// Sunucu-otoriter model korunur: istemci niyet (basılı tuş) gönderir,
// pozisyonu sunucu hesaplar. Oyuncu bir bölge sınırını geçince HANDOFF
// olur: kaynak bölge oyuncuyu hedef bölgeye devreder, oturumun bölge
// ref'i güncellenir. Tick DIŞARIDAN gelir (tüm bölgelere) — testler
// deterministik.
package world

import "shardlands/pkg/actor"

const (
	Width    = 800.0 // dünya sınırları (px)
	Height   = 600.0
	Speed    = 200.0 // px/s
	TickRate = 20    // Tick/s — dt = 1/TickRate

	// Grid: dünya Cols×Rows bölgeye bölünür. Sınırlar x=RegionW*k,
	// y=RegionH*k. 2×2 = 4 bölge (dört çeyrek).
	Cols    = 2
	Rows    = 2
	RegionW = Width / Cols  // 400
	RegionH = Height / Rows // 300

	maxChatLen  = 120
	bubbleTicks = 4 * TickRate

	GatherRadius = 48.0
	RespawnTicks = 10 * TickRate
)

// Event sözleşmesi (read model'ler buna bağlanır). Hareket BİLEREK event
// değildir; log kalıcı gerçekler içindir.
const (
	ChatStream    = "chat"
	EventChatSaid = "ChatSaid"
)

type ChatSaid struct {
	PlayerID string `json:"playerId"`
	Name     string `json:"name"`
	Text     string `json:"text"`
}

// nodeLayout: hub'ın sabit kaynak yerleşimi (dünya koordinatlarında).
// Her node, konumuna göre bir bölgeye düşer (router bunu hesaplar).
var nodeLayout = []NodeState{
	{ID: "n1", Kind: "wood", X: 150, Y: 150},
	{ID: "n2", Kind: "wood", X: 650, Y: 150},
	{ID: "n3", Kind: "crystal", X: 150, Y: 450},
	{ID: "n4", Kind: "crystal", X: 650, Y: 450},
	{ID: "n5", Kind: "wood", X: 400, Y: 180},
	{ID: "n6", Kind: "crystal", X: 400, Y: 420},
}

// ---- bölge aktörüne gönderilen mesajlar ----

// Join: oyuncu bölgeye giriyor. X,Y giriş konumu; In, handoff'ta taşınan
// basılı tuş durumu (taze girişte sıfır). Session, Snapshot'ların
// gideceği Ref.
type Join struct {
	PlayerID string
	Name     string
	Session  *actor.Ref
	X, Y     float64
	In       Input
}

// Leave: oyuncu ayrıldı (bilinmeyen id sessizce yok sayılır — idempotent).
type Leave struct {
	PlayerID string
}

// Input: basılı tuş durumu.
type Input struct {
	PlayerID              string
	Up, Down, Left, Right bool
}

// Chat / Gather: bölge aktöründe işlenir (sohbet balonu ve toplama
// bölgeye özel).
type Chat struct {
	PlayerID string
	Text     string
}
type Gather struct {
	PlayerID string
}

// Tick: bölge simülasyonunu bir adım ilerlet.
type Tick struct{}

// ---- oturuma yayınlanan mesajlar ----

// Snapshot, bir bölgenin tick sonrası durumudur: yalnız O BÖLGEDEKİ
// oyuncular ve node'lar. RegionID/Shard, istemcinin nerede olduğunu
// göstermesi için. (Bölgeler arası görüş — border AOI — bilinçli kapsam
// dışı; Faz 5+.)
type Snapshot struct {
	Tick     uint64
	RegionID string
	Shard    string
	Players  []PlayerState
	Nodes    []NodeState
}

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
	Bubble string
}

// AssignedRegion: router/bölge, oturuma "artık şu bölgedesin" der.
// Oturum bundan sonra input'u Ref'e gönderir. Handoff'un istemci-görünmez
// yönlendirmesi budur.
type AssignedRegion struct {
	RegionID string
	Shard    string
	Ref      *actor.Ref
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
