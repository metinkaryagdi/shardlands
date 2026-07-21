// Package inventory, oyuncu envanterlerinin READ MODEL'idir: inv-*
// stream'lerindeki event'lerden (şimdilik ResourceGathered; takas
// event'leri saga ile eklenecek) oyuncu→tür→adet sayımları türetir.
//
// Envanterin "gerçeği" event log'dur; bu paket yalnızca hızlı sorgu
// için bir görünümdür ve her açılışta sıfırdan yeniden kurulur.
package inventory

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	"shardlands/pkg/es"
	"shardlands/services/world"
)

type Inventory struct {
	store *es.Store

	mu     sync.RWMutex
	counts map[string]map[string]int // playerID → kind → adet

	stop chan struct{}
	done chan struct{}
}

// New, projection'ı başlatır (arka planda event akışını izler).
func New(store *es.Store) *Inventory {
	inv := &Inventory{
		store:  store,
		counts: map[string]map[string]int{},
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(inv.done)
		es.Project(store, inv.stop, inv.apply)
	}()
	return inv
}

func (inv *Inventory) apply(evs []es.Event) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	for _, e := range evs {
		if !strings.HasPrefix(e.Stream, "inv-") {
			continue
		}
		switch e.Type {
		case world.EventResourceGathered:
			var g world.ResourceGathered
			if err := json.Unmarshal(e.Data, &g); err != nil {
				log.Printf("inventory: bad event %d: %v", e.Global, err)
				continue
			}
			inv.add(g.PlayerID, g.Kind, g.Amount)
		}
	}
}

func (inv *Inventory) add(playerID, kind string, n int) {
	m := inv.counts[playerID]
	if m == nil {
		m = map[string]int{}
		inv.counts[playerID] = m
	}
	m[kind] += n
	if m[kind] <= 0 {
		delete(m, kind)
	}
}

// Get, oyuncunun envanterinin kopyasını döner (boşsa boş map).
func (inv *Inventory) Get(playerID string) map[string]int {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	out := map[string]int{}
	for kind, n := range inv.counts[playerID] {
		out[kind] = n
	}
	return out
}

// Close, projection'ı durdurur ve bitmesini bekler.
func (inv *Inventory) Close() {
	close(inv.stop)
	<-inv.done
}
