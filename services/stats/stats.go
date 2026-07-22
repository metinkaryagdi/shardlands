// Package stats, global oyun sayaçlarının READ MODEL'idir. "Toplam
// toplanan kaynak" bir G-COUNTER olarak tutulur: her ResourceGathered
// event'i bu düğümün bileşenini artırır.
//
// Neden CRDT? Bu sayaç, Faz 3'te birden çok world shard'ına bölününce
// anlaşma gerektirmez: her shard kendi bileşenini bağımsız artırır,
// periyodik merge (element-bazlı max) toplam üzerinde yakınsar —
// lider/çoğunluk yok. Tek düğümde bugün tek bileşen var; yapı Faz 3'e
// hazır. (Karşıtlık: bir arena maçının sonucu Raft ister; bu sayaç
// istemez.)
//
// Çevrimiçi oyuncu sayısı bilinçli olarak burada DEĞİL: o monoton
// değildir (oyuncular ayrılır) ve bir düğüm LEAVE yazamadan çökerse
// G-Counter şişerdi — anlık canlı sayım için yanlış araç. Gateway onu
// basit bir gauge olarak tutar.
package stats

import (
	"encoding/json"
	"log"
	"sync"

	"shardlands/pkg/bus"
	"shardlands/pkg/crdt"
	"shardlands/pkg/es"
	"shardlands/services/inventory"
	"shardlands/services/outbox"
)

type Stats struct {
	nodeID string

	mu       sync.RWMutex
	gathered *crdt.GCounter

	sub bus.Subscription
}

// New, bus'tan tüketen projection'ı başlatır. nodeID, bu replikanın
// G-Counter bileşeninin anahtarıdır (Faz 3'te shard başına farklı).
func New(b bus.Bus, nodeID string) (*Stats, error) {
	s := &Stats{nodeID: nodeID, gathered: crdt.NewGCounter()}
	sub, err := outbox.Consume(b, "stats", s.apply)
	if err != nil {
		return nil, err
	}
	s.sub = sub
	return s, nil
}

func (s *Stats) apply(e es.Event) error {
	if e.Type != inventory.EventGathered {
		return nil
	}
	var g inventory.Gathered
	if err := json.Unmarshal(e.Data, &g); err != nil {
		log.Printf("stats: bad gather event %d: %v", e.Global, err)
		return nil
	}
	s.mu.Lock()
	s.gathered.Increment(s.nodeID, uint64(g.Amount))
	s.mu.Unlock()
	return nil
}

// TotalGathered, tüm zamanların toplam toplanan kaynak sayısı.
func (s *Stats) TotalGathered() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gathered.Value()
}

// GatheredState, G-Counter'ın bileşen haritası (gözlem/Faz 3 merge için).
func (s *Stats) GatheredState() map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gathered.State()
}

func (s *Stats) Close() {
	if s.sub != nil {
		s.sub.Stop()
	}
}
