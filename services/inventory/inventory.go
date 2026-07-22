package inventory

import (
	"strings"
	"sync"

	"shardlands/pkg/bus"
	"shardlands/pkg/es"
	"shardlands/services/outbox"
)

// Inventory, envanterlerin READ MODEL'idir: inv-* stream'lerindeki tüm
// event'lerden (gather + takas hareketleri) oyuncu başına bakiye
// türetir. Envanterin "gerçeği" event log'dur; bu paket hızlı sorgu
// için bir görünümdür ve her açılışta sıfırdan yeniden kurulur.
type Inventory struct {
	mu   sync.RWMutex
	bals map[string]*Balance // playerID → bakiye

	sub bus.Subscription
}

// New, bus'tan tüketen projection'ı başlatır (Faz 4: doğrudan event
// store yerine event bus; at-least-once teslim, dedupe outbox.Consume'da).
func New(b bus.Bus) (*Inventory, error) {
	inv := &Inventory{bals: map[string]*Balance{}}
	sub, err := outbox.Consume(b, "inventory", inv.apply)
	if err != nil {
		return nil, err
	}
	inv.sub = sub
	return inv, nil
}

func (inv *Inventory) apply(e es.Event) error {
	if !strings.HasPrefix(e.Stream, "inv-") {
		return nil // log paylaşımlı: bizim olmayan stream'leri atla
	}
	playerID := strings.TrimPrefix(e.Stream, "inv-")
	inv.mu.Lock()
	defer inv.mu.Unlock()
	b := inv.bals[playerID]
	if b == nil {
		nb := newBalance()
		b = &nb
		inv.bals[playerID] = b
	}
	applyTo(b, e)
	return nil
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

// Close, projection'ı durdurur.
func (inv *Inventory) Close() {
	if inv.sub != nil {
		inv.sub.Stop()
	}
}
