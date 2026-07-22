package world

import (
	"fmt"
	"sort"
	"sync"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/pkg/hashring"
)

// ringReplicas: hashring vnode sayısı (bölge→shard dengesi için).
const ringReplicas = 128

// Router, bölge yerleşiminin ve yönlendirmesinin koordinatörüdür:
//   - consistent hashing ile bölge→shard node eşlemesi (hangi shard
//     hangi bölgeyi barındırır),
//   - bölge id → bölge aktörü ref eşlemesi (input'un gideceği yer),
//   - shard'ların "ayakta mı" durumu (CAP deneyi: bir shard'ı izole
//     etmek, bölgelerini kullanılamaz yapar).
//
// regions/shardOf kurulumdan SONRA değişmez (yalnız okunur); shardUp
// çalışma anında değişir (kilitli).
type Router struct {
	ring    *hashring.Ring
	regions map[string]*actor.Ref
	shardOf map[string]string

	mu      sync.RWMutex
	shardUp map[string]bool
}

// NewHub, shard node'ları ve bölgeleri kurar: her bölge için bir aktör
// spawn eder, consistent hashing ile bölgeyi bir shard'a atar. events
// nil olabilir (test).
func NewHub(sys *actor.System, events *es.Store, shards []string) (*Router, error) {
	r := &Router{
		ring:    hashring.New(ringReplicas),
		regions: map[string]*actor.Ref{},
		shardOf: map[string]string{},
		shardUp: map[string]bool{},
	}
	r.ring.Add(shards...)
	for _, s := range shards {
		r.shardUp[s] = true
	}

	for col := 0; col < Cols; col++ {
		for row := 0; row < Rows; row++ {
			id := regionID(col, row)
			shard := r.ring.Get(id)
			r.shardOf[id] = shard
			ref, err := sys.Spawn(regionProps(id, col, row, shard, events, r))
			if err != nil {
				return nil, err
			}
			r.regions[id] = ref
		}
	}
	return r, nil
}

func regionID(col, row int) string { return fmt.Sprintf("r-%d-%d", col, row) }

// RegionAt, dünya koordinatındaki noktanın bölge id'sini döner.
func RegionAt(x, y float64) string {
	col := int(x / RegionW)
	if col >= Cols {
		col = Cols - 1
	}
	if col < 0 {
		col = 0
	}
	row := int(y / RegionH)
	if row >= Rows {
		row = Rows - 1
	}
	if row < 0 {
		row = 0
	}
	return regionID(col, row)
}

// Ref, bölge aktörünün ref'i (yoksa nil).
func (r *Router) Ref(regionID string) *actor.Ref { return r.regions[regionID] }

// ShardOf, bölgeyi barındıran shard node.
func (r *Router) ShardOf(regionID string) string { return r.shardOf[regionID] }

// Refs, tüm bölge aktörleri (deterministik sıra — tick döngüsü için).
func (r *Router) Refs() []*actor.Ref {
	ids := make([]string, 0, len(r.regions))
	for id := range r.regions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	refs := make([]*actor.Ref, len(ids))
	for i, id := range ids {
		refs[i] = r.regions[id]
	}
	return refs
}

// SpawnRegion, bir oyuncunun giriş bölgesini (x,y'ye göre) ve ref'ini
// döner; oturum PreStart'ta bunu kullanır.
func (r *Router) SpawnRegion(x, y float64) (regionID, shard string, ref *actor.Ref) {
	id := RegionAt(x, y)
	return id, r.shardOf[id], r.regions[id]
}

// ShardUp / SetShardUp: CAP deneyi. Bir shard "down" ise bölgeleri
// kullanılamaz (handoff hedefi olamaz; yeni giriş alamaz).
func (r *Router) ShardUp(shard string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.shardUp[shard]
}

func (r *Router) SetShardUp(shard string, up bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shardUp[shard] = up
}

// RegionShardUp, bölgeyi barındıran shard ayakta mı?
func (r *Router) RegionShardUp(regionID string) bool {
	return r.ShardUp(r.shardOf[regionID])
}
