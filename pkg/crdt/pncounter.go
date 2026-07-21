package crdt

// PNCounter, hem artabilen hem azalabilen bir sayaçtır. Hile: G-Counter
// yalnız artabildiği için, İKİ G-Counter tutulur — P (artışlar) ve N
// (azalışlar) — ve değer P−N olur. İkisi de monoton arttığından her biri
// CRDT kalır; farkları da yakınsar.
//
// Ne zaman? Değerin iki yöne de gidebildiği yerlerde: bir ekonomideki net
// bakiye, oylar (+1/−1), ya da (kabaca) çevrimiçi oyuncu sayısı
// (join=+1, leave=−1). Çevrimiçi sayısı için uyarı: bir düğüm LEAVE
// yazamadan çökerse azalış kaybolur (sayaç şişer) — bu yüzden "kesin
// canlı sayım" için gözlemlenen-küme (OR-Set) tarzı bir CRDT daha
// doğrudur; PN-Counter kümülatif akışlar için idealdir.
type PNCounter struct {
	p *GCounter // artışlar
	n *GCounter // azalışlar
}

func NewPNCounter() *PNCounter {
	return &PNCounter{p: NewGCounter(), n: NewGCounter()}
}

// Increment, node adına delta kadar artırır.
func (c *PNCounter) Increment(node string, delta uint64) { c.p.Increment(node, delta) }

// Decrement, node adına delta kadar azaltır.
func (c *PNCounter) Decrement(node string, delta uint64) { c.n.Increment(node, delta) }

// Value, değerdir: toplam artış − toplam azalış. int64 (negatif olabilir).
func (c *PNCounter) Value() int64 {
	return int64(c.p.Value()) - int64(c.n.Value())
}

// Merge, iki alt-sayacı ayrı ayrı birleştirir. G-Counter merge'ü
// CRDT olduğundan bileşimi de CRDT'dir.
func (c *PNCounter) Merge(other *PNCounter) {
	c.p.Merge(other.p)
	c.n.Merge(other.n)
}

func (c *PNCounter) Clone() *PNCounter {
	return &PNCounter{p: c.p.Clone(), n: c.n.Clone()}
}
