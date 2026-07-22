// Package shard, her shard'ı bir RAFT GRUBU olarak yönetir: shard'ın
// bölgelerini simüle etme yetkisi, grubun LİDERİNE aittir.
//
// Neden Raft? Faz 3'e kadar bir shard tek kopyaydı — o süreç çökerse
// bölgeleri kaybolurdu ve iki süreç aynı bölgeyi simüle etmeye kalksa
// split-brain olurdu. Raft ikisini birden çözer: çoğunluk tek bir lider
// üzerinde anlaşır (split-brain yok) ve lider çökünce failover ile
// yenisi devralır (dayanıklılık).
//
// Kullanılabilirlik tanımı önemlidir: "bir düğüm kendini lider sanıyor"
// YETMEZ — bölünmüş bir lider commit edemez. Bu yüzden Available(),
// raft.Node.QuorumActive ile "çoğunlukla teması süren bir lider var mı"
// diye sorar. CAP'in C tarafı: çoğunluk yoksa shard KULLANILAMAZ
// (bölgelerine giriş kapanır) — tutarlılık için kullanılabilirlikten
// vazgeçilir. (Karşıt örnek: CRDT sayaç bölünmede çalışmaya devam eder.)
package shard

import (
	"fmt"
	"path/filepath"
	"time"

	"shardlands/pkg/raft"
	"shardlands/pkg/raftstore"
)

// Varsayılan zamanlamalar: heartbeat << seçim zaman aşımı.
const (
	defaultElectionMin = 150 * time.Millisecond
	defaultElectionMax = 300 * time.Millisecond
	defaultHeartbeat   = 50 * time.Millisecond
	defaultTick        = 10 * time.Millisecond
	// quorumWindow: bu süre içinde çoğunlukla temas yoksa lider "aktif"
	// sayılmaz (birkaç heartbeat kaçırma toleransı).
	quorumWindow = 4 * defaultHeartbeat
)

// Group, tek bir shard'ın replika kümesidir.
type Group struct {
	ID    string
	ids   []string
	nodes map[string]*raft.Node
	nw    *raft.Network

	stores []*raftstore.Store // kalıcı depo kullanıldıysa
}

// Options, grup kurulumunu ayarlar.
type Options struct {
	Replicas int    // varsayılan 3
	DataDir  string // boşsa in-memory storage (test)
	Sync     bool   // kalıcı depoda fsync
	// Zamanlama sıkıştırma (testler için); sıfırsa varsayılanlar.
	ElectionMin, ElectionMax, Heartbeat, Tick time.Duration
}

func (o *Options) withDefaults() {
	if o.Replicas <= 0 {
		o.Replicas = 3
	}
	if o.ElectionMin <= 0 {
		o.ElectionMin = defaultElectionMin
	}
	if o.ElectionMax <= o.ElectionMin {
		o.ElectionMax = 2 * o.ElectionMin
	}
	if o.Heartbeat <= 0 {
		o.Heartbeat = defaultHeartbeat
	}
	if o.Tick <= 0 {
		o.Tick = defaultTick
	}
}

// NewGroup, shard için replicas düğümlü bir Raft grubu kurar. Gruplar
// birbirinden yalıtıktır (her grubun kendi ağı) — bir shard'ı izole etmek
// diğerini etkilemez.
func NewGroup(shardID string, opts Options) (*Group, error) {
	opts.withDefaults()
	g := &Group{
		ID:    shardID,
		nodes: map[string]*raft.Node{},
		nw:    raft.NewNetwork(),
	}
	for i := 0; i < opts.Replicas; i++ {
		g.ids = append(g.ids, fmt.Sprintf("%s-r%d", shardID, i))
	}
	for _, id := range g.ids {
		var peers []string
		for _, p := range g.ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		cfg := raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          g.nw.Transport(id),
			ElectionTimeoutMin: opts.ElectionMin,
			ElectionTimeoutMax: opts.ElectionMax,
			HeartbeatInterval:  opts.Heartbeat,
			TickInterval:       opts.Tick,
		}
		if opts.DataDir != "" {
			st, err := raftstore.Open(filepath.Join(opts.DataDir, shardID, id), opts.Sync)
			if err != nil {
				g.Stop()
				return nil, err
			}
			g.stores = append(g.stores, st)
			cfg.Storage = st
		}
		n, err := raft.NewNode(cfg)
		if err != nil {
			g.Stop()
			return nil, err
		}
		g.nodes[id] = n
		g.nw.Register(id, n)
	}
	return g, nil
}

// Leader, çoğunlukla teması süren lideri döner (yoksa "", false).
// Bölünmüş/kendini lider sanan düğüm buradan DÖNMEZ.
func (g *Group) Leader() (string, bool) {
	for _, id := range g.ids {
		if n := g.nodes[id]; n != nil && n.QuorumActive(quorumWindow) {
			return id, true
		}
	}
	return "", false
}

// Available, shard'ın hizmet verebilir olup olmadığı (aktif lider var mı).
func (g *Group) Available() bool {
	_, ok := g.Leader()
	return ok
}

// Propose, komutu gruba (liderine) önerir. Lider yoksa/çoğunluk yoksa
// false döner — azınlıkta yazma kabul edilmez.
func (g *Group) Propose(cmd []byte) bool {
	id, ok := g.Leader()
	if !ok {
		return false
	}
	_, _, accepted := g.nodes[id].Propose(cmd)
	return accepted
}

// Partition, grubu ayrık parçalara böler (CAP deneyi). Listelenmeyen
// düğümler ortak grupta kalır.
func (g *Group) Partition(groups ...[]string) { g.nw.Partition(groups...) }

// IsolateAll, her düğümü tek başına bırakır: hiçbir tarafta çoğunluk
// kalmaz → lider yok → shard kullanılamaz.
func (g *Group) IsolateAll() {
	parts := make([][]string, 0, len(g.ids))
	for _, id := range g.ids {
		parts = append(parts, []string{id})
	}
	g.nw.Partition(parts...)
}

// Heal, tüm bölünmeleri kaldırır.
func (g *Group) Heal() { g.nw.Heal() }

// IDs, replika id'leri.
func (g *Group) IDs() []string { return append([]string(nil), g.ids...) }

func (g *Group) Stop() {
	for _, n := range g.nodes {
		n.Stop()
	}
	for _, st := range g.stores {
		st.Close()
	}
}

// ---- Manager: shard id → grup ----

// Manager, tüm shard gruplarını tutar ve world.Router'a kullanılabilirlik
// bilgisi sağlar (Availability arayüzü).
type Manager struct {
	groups map[string]*Group
	order  []string
}

func NewManager(shards []string, opts Options) (*Manager, error) {
	m := &Manager{groups: map[string]*Group{}}
	for _, s := range shards {
		g, err := NewGroup(s, opts)
		if err != nil {
			m.Stop()
			return nil, err
		}
		m.groups[s] = g
		m.order = append(m.order, s)
	}
	return m, nil
}

// Available, world.Router'ın sorduğu soru: bu shard hizmet verebilir mi?
func (m *Manager) Available(shard string) bool {
	g, ok := m.groups[shard]
	if !ok {
		return false
	}
	return g.Available()
}

// Group, shard'ın grubunu döner (deneyler/testler için).
func (m *Manager) Group(shard string) *Group { return m.groups[shard] }

// Shards, yönetilen shard id'leri.
func (m *Manager) Shards() []string { return append([]string(nil), m.order...) }

// WaitReady, tüm shard'larda aktif lider oluşana kadar bekler.
func (m *Manager) WaitReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, s := range m.order {
			if !m.groups[s].Available() {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (m *Manager) Stop() {
	for _, g := range m.groups {
		g.Stop()
	}
}
