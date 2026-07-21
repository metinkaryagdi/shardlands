package crdt

// GCounter, yalnız-artan (grow-only) bir sayaçtır. Her replika (node)
// kendi bileşenini artırır; hiçbir replika başkasının bileşenine
// dokunmaz. Değer = tüm bileşenlerin toplamı. Merge = bileşen-bazlı max.
//
// Neden max ile birleşir? Her bileşen tek bir düğüm tarafından, yalnız
// ARTARAK yazıldığından, iki gözlemden büyüğü daha güncel olandır. Max
// almak commutative/associative/idempotent'tir — semilattice budur; bu
// yüzden replikalar mesaj sırası/tekrarından bağımsız yakınsar.
//
// Thread-safe DEĞİLDİR: bir replikanın durumu tek bir sürecin/aktörün
// elindedir (actor modeliyle doğal). Eşzamanlı erişim gerekiyorsa
// çağıran senkronize etmelidir.
type GCounter struct {
	counts map[string]uint64 // node → o düğümün toplam artışı
}

func NewGCounter() *GCounter {
	return &GCounter{counts: map[string]uint64{}}
}

// Increment, node bileşenini delta kadar artırır. Yalnızca replikanın
// KENDİ düğüm id'siyle çağrılmalıdır (her düğüm kendi bileşenini yazar).
func (g *GCounter) Increment(node string, delta uint64) {
	g.counts[node] += delta
}

// Value, sayacın değeridir (tüm bileşenlerin toplamı).
func (g *GCounter) Value() uint64 {
	var sum uint64
	for _, c := range g.counts {
		sum += c
	}
	return sum
}

// Merge, other'ı bu sayaca katar: her düğüm için max(bizim, onun).
// Commutative, associative, idempotent — CRDT sözleşmesi.
func (g *GCounter) Merge(other *GCounter) {
	for node, c := range other.counts {
		if c > g.counts[node] {
			g.counts[node] = c
		}
	}
}

// Clone, bağımsız bir kopya döner (bir replikanın durumunu ağ üzerinden
// göndermeden önce dondurmak için).
func (g *GCounter) Clone() *GCounter {
	c := &GCounter{counts: make(map[string]uint64, len(g.counts))}
	for node, v := range g.counts {
		c.counts[node] = v
	}
	return c
}

// State, bileşen haritasının kopyasıdır (serialize/gözlem için).
func (g *GCounter) State() map[string]uint64 {
	return g.Clone().counts
}
