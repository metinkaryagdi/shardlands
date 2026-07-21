package inventory

import (
	"strings"
	"sync"

	"shardlands/pkg/es"
)

// Inventory, envanterlerin READ MODEL'idir: inv-* stream'lerindeki tüm
// event'lerden (gather + takas hareketleri) oyuncu başına bakiye
// türetir. Envanterin "gerçeği" event log'dur; bu paket hızlı sorgu
// için bir görünümdür ve her açılışta sıfırdan yeniden kurulur.
type Inventory struct {
	store *es.Store

	mu   sync.RWMutex
	bals map[string]*Balance // playerID → bakiye

	stop chan struct{}
	done chan struct{}
}

// New, projection'ı başlatır (arka planda event akışını izler).
func New(store *es.Store) *Inventory {
	inv := &Inventory{
		store: store,
		bals:  map[string]*Balance{},
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
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
			continue // log paylaşımlı: bizim olmayan stream'leri atla
		}
		playerID := strings.TrimPrefix(e.Stream, "inv-")
		b := inv.bals[playerID]
		if b == nil {
			nb := newBalance()
			b = &nb
			inv.bals[playerID] = b
		}
		applyTo(b, e)
	}
}

// Get, oyuncunun HARCANABİLİR (available) bakiyesinin kopyasını döner.
// Rezerve edilen mallar burada görünmez — takas askıdayken oyuncunun
// "kullanılabilir çantası" budur.
func (inv *Inventory) Get(playerID string) map[string]int {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	out := map[string]int{}
	if b := inv.bals[playerID]; b != nil {
		for kind, n := range b.Available {
			if n != 0 {
				out[kind] = n
			}
		}
	}
	return out
}

// Reserved, oyuncunun tür başına tutulan (rezerve) miktarını döner.
func (inv *Inventory) Reserved(playerID string) map[string]int {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	out := map[string]int{}
	if b := inv.bals[playerID]; b != nil {
		for kind, n := range b.Reserved {
			if n != 0 {
				out[kind] = n
			}
		}
	}
	return out
}

// Close, projection'ı durdurur ve bitmesini bekler.
func (inv *Inventory) Close() {
	close(inv.stop)
	<-inv.done
}
