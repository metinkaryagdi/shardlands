package matchmaking

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/services/arena"
)

// Maç saga'sının event sözleşmesi. Trade saga'sında olduğu gibi saga'nın
// KENDİ event log'u aynı zamanda durumu ve denetim izidir.
const (
	EventMatchFormed      = "MatchFormed"
	EventArenaProvisioned = "ArenaProvisioned"
	EventPlayersAssigned  = "PlayersAssigned"
	EventMatchCancelled   = "MatchCancelled"
)

// MatchStream, bir maçın stream adı.
func MatchStream(matchID string) string { return "match-" + matchID }

// MatchFormed, eşleşmenin veri şekli.
type MatchFormed struct {
	MatchID string   `json:"matchId"`
	Mode    string   `json:"mode"`
	Players []string `json:"players"`
	Teams   []int    `json:"teams"`
}

// MatchCancelled, iptal nedeni.
type MatchCancelled struct {
	MatchID string `json:"matchId"`
	Reason  string `json:"reason"`
}

// QueuedPlayer, kuyrukta bekleyen oyuncu (oturum ref'iyle).
type QueuedPlayer struct {
	ID      string
	Name    string
	Session *actor.Ref
}

// Assigner, eşleşen oyuncuyu arenaya yönlendiren bileşendir (gateway
// uygular: oturumu arena moduna alır). Hata dönerse saga TELAFİ eder.
type Assigner interface {
	Assign(playerID string, h *Handle, team int) error
	// Release, telafi: oyuncuyu arenadan geri al (hub'a döndür).
	Release(playerID string)
}

// Matcher, kuyrukları tutar ve maç saga'sını sürer.
type Matcher struct {
	store    *es.Store
	prov     Provisioner
	assigner Assigner

	mu       sync.Mutex
	queues   map[string][]QueuedPlayer // mode → kuyruk
	registry map[string]QueuedPlayer   // playerID → kayıt (oturum ref'i)
	inQueue  map[string]string         // playerID → mode (çifte kuyruk yok)

	nextID  atomic.Int64
	matches atomic.Int64 // başarıyla kurulan maç sayısı (gözlem)
}

// NewMatcher, eşleştiriciyi kurar. store nil olabilir (denetim izi
// yazılmaz); assigner nil ise atama adımı no-op'tur.
func NewMatcher(store *es.Store, prov Provisioner, assigner Assigner) *Matcher {
	return &Matcher{
		store: store, prov: prov, assigner: assigner,
		queues:   map[string][]QueuedPlayer{},
		registry: map[string]QueuedPlayer{},
		inQueue:  map[string]string{},
	}
}

// Register/Unregister: gateway, bağlanan oyuncuyu oturum ref'iyle
// kaydeder (gRPC üzerinden ref taşınamaz).
func (m *Matcher) Register(p QueuedPlayer) {
	m.mu.Lock()
	m.registry[p.ID] = p
	m.mu.Unlock()
}

func (m *Matcher) Unregister(playerID string) {
	m.mu.Lock()
	delete(m.registry, playerID)
	// Kuyruktaysa çıkar (bağlantısı kopan oyuncu eşleşmemeli).
	if mode, ok := m.inQueue[playerID]; ok {
		q := m.queues[mode]
		for i, p := range q {
			if p.ID == playerID {
				m.queues[mode] = append(q[:i], q[i+1:]...)
				break
			}
		}
		delete(m.inQueue, playerID)
	}
	m.mu.Unlock()
}

// Enqueue, oyuncuyu moda göre kuyruğa alır ve sırasını döner.
// İdempotent: zaten kuyruktaysa mevcut sırasını döner. Yeterli oyuncu
// birikirse maç saga'sını başlatır.
func (m *Matcher) Enqueue(playerID, mode string) (int, error) {
	size := teamSizeFor(mode)
	if size == 0 {
		return 0, fmt.Errorf("matchmaking: unknown mode %q", mode)
	}
	m.mu.Lock()
	if cur, ok := m.inQueue[playerID]; ok {
		q := m.queues[cur]
		for i, p := range q {
			if p.ID == playerID {
				m.mu.Unlock()
				return i + 1, nil
			}
		}
	}
	p, ok := m.registry[playerID]
	if !ok {
		p = QueuedPlayer{ID: playerID, Name: playerID}
	}
	m.queues[mode] = append(m.queues[mode], p)
	m.inQueue[playerID] = mode
	pos := len(m.queues[mode])
	match := m.formLocked(mode)
	m.mu.Unlock()

	if match != nil {
		go m.runSaga(*match)
	}
	return pos, nil
}

// formLocked, yeterli oyuncu varsa kuyruktan bir maç ÇEKER (kilit
// altında: aynı oyuncu iki maça düşemez).
func (m *Matcher) formLocked(mode string) *match {
	need := 2 * teamSizeFor(mode)
	q := m.queues[mode]
	if len(q) < need {
		return nil
	}
	picked := make([]QueuedPlayer, need)
	copy(picked, q[:need])
	m.queues[mode] = append([]QueuedPlayer(nil), q[need:]...)
	for _, p := range picked {
		delete(m.inQueue, p.ID)
	}

	mt := &match{
		id:      fmt.Sprintf("m%d", m.nextID.Add(1)),
		mode:    mode,
		players: picked,
		teams:   make([]int, need),
	}
	// İlk yarı takım 0, ikinci yarı takım 1.
	for i := range picked {
		if i >= need/2 {
			mt.teams[i] = 1
		}
	}
	return mt
}

type match struct {
	id      string
	mode    string
	players []QueuedPlayer
	teams   []int
}

// runSaga: maç kurulum saga'sı.
//
//	Adım 1: arena provision et
//	Adım 2: oyuncuları arenaya ata
//
// Telafiler: provision başarısızsa oyuncular kuyruğa İADE edilir;
// atama başarısızsa arena YIKILIR ve atanmışlar geri alınıp herkes
// kuyruğa iade edilir. Böylece "boş arena sızıntısı" veya "kuyrukta
// kaybolan oyuncu" oluşmaz.
func (m *Matcher) runSaga(mt match) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	m.appendMatchFormed(mt)

	specs := make([]arena.PlayerSpec, len(mt.players))
	for i, p := range mt.players {
		specs[i] = arena.PlayerSpec{ID: p.ID, Name: p.Name, Team: mt.teams[i], Session: p.Session}
	}

	// Adım 1: provision.
	h, err := m.prov.Provision(ctx, ArenaSpec{
		ID:      "arena-" + mt.id,
		Mode:    arena.Mode(mt.mode),
		Players: specs,
		OnEnd:   func(r arena.Result) { m.onMatchEnd(mt, r) },
	})
	if err != nil {
		m.cancel(mt, "provision: "+err.Error(), nil) // TELAFİ: kuyruğa iade
		return
	}
	m.appendPhase(mt.id, EventArenaProvisioned)

	// Adım 2: atama.
	assigned := make([]string, 0, len(mt.players))
	for i, p := range mt.players {
		if m.assigner == nil {
			continue
		}
		if err := m.assigner.Assign(p.ID, h, mt.teams[i]); err != nil {
			// TELAFİ: atananları geri al, arenayı yık, herkesi kuyruğa iade et.
			for _, id := range assigned {
				m.assigner.Release(id)
			}
			m.prov.Destroy(ctx, h.ID)
			m.cancel(mt, "assign "+p.ID+": "+err.Error(), nil)
			return
		}
		assigned = append(assigned, p.ID)
	}

	m.appendPhase(mt.id, EventPlayersAssigned)
	m.matches.Add(1)
}

// onMatchEnd, maç bitince arenayı temizler (oyuncuların hub'a dönüşü
// Assigner.Release ile).
func (m *Matcher) onMatchEnd(mt match, r arena.Result) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if m.assigner != nil {
		for _, p := range mt.players {
			m.assigner.Release(p.ID)
		}
	}
	m.prov.Destroy(ctx, r.ArenaID)
}

// cancel, saga'yı iptal eder: oyuncuları kuyruğa iade edip olayı yazar.
func (m *Matcher) cancel(mt match, reason string, _ error) {
	m.mu.Lock()
	// Kuyruğun BAŞINA iade: sıralarını kaybetmesinler.
	restored := append([]QueuedPlayer(nil), mt.players...)
	m.queues[mt.mode] = append(restored, m.queues[mt.mode]...)
	for _, p := range mt.players {
		m.inQueue[p.ID] = mt.mode
	}
	m.mu.Unlock()

	data, _ := json.Marshal(MatchCancelled{MatchID: mt.id, Reason: reason})
	m.append(mt.id, EventMatchCancelled, data)
	log.Printf("matchmaking: match %s cancelled: %s", mt.id, reason)
}

// ---- denetim izi ----

func (m *Matcher) appendMatchFormed(mt match) {
	ids := make([]string, len(mt.players))
	for i, p := range mt.players {
		ids[i] = p.ID
	}
	data, _ := json.Marshal(MatchFormed{
		MatchID: mt.id, Mode: mt.mode, Players: ids, Teams: mt.teams,
	})
	m.append(mt.id, EventMatchFormed, data)
}

func (m *Matcher) appendPhase(matchID, typ string) { m.append(matchID, typ, nil) }

func (m *Matcher) append(matchID, typ string, data []byte) {
	if m.store == nil {
		return
	}
	if _, err := m.store.Append(MatchStream(matchID), es.AnyVersion,
		es.EventData{Type: typ, Data: data}); err != nil {
		log.Printf("matchmaking: append %s: %v", typ, err)
	}
}

// ---- gözlem ----

// QueueLen, moddaki kuyruk uzunluğu.
func (m *Matcher) QueueLen(mode string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queues[mode])
}

// Matches, kurulan maç sayısı.
func (m *Matcher) Matches() int64 { return m.matches.Load() }

func teamSizeFor(mode string) int {
	switch mode {
	case string(arena.Mode1v1):
		return 1
	case string(arena.Mode2v2):
		return 2
	default:
		return 0
	}
}
