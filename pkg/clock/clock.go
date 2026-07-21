// Package clock, dağıtık sistemler için mantıksal saatler sağlar:
// Lamport clock ve vector clock.
//
// Fiziksel saatler dağıtık sıralama için güvenilmezdir (skew, NTP
// sıçramaları). Lamport'un gözlemi (1978): çoğu zaman "saat kaçta?"
// değil "hangisi önce?" sorusunu soruyoruz ve bunun cevabı fiziksel
// zaman değil NEDENSELLİKTİR (happens-before, →): aynı süreçte ardışık
// olaylar, mesajın gönderimi → alımı ve bunların geçişli kapanışı.
//
//   - Lamport clock: e1 → e2 ise L(e1) < L(e2). TERSİ DOĞRU DEĞİL —
//     L(e1) < L(e2), nedensellik kanıtlamaz. Tek uint64; ucuz, toplam
//     sıralama kırıcı (tiebreak) olarak iyi.
//   - Vector clock: e1 → e2 ⟺ V(e1) < V(e2). Nedenselliği TAM
//     karakterize eder; eşzamanlılığı (Concurrent) TESPİT EDEBİLİR.
//     Bedeli düğüm başına bir sayaç (O(N) alan).
//
// Shardlands'te kullanım (Faz 2+): CRDT çakışma tespiti vector clock;
// event log'larında deterministik sıralama Lamport + düğüm id'si.
// Uzantı notu: fiziksel zamana yakınlık + nedensellik birlikte
// gerekirse Hybrid Logical Clock (HLC) — bilinçli kapsam dışı.
package clock

import "sync/atomic"

// Lamport, thread-safe skaler mantıksal saattir.
type Lamport struct {
	c atomic.Uint64
}

// Tick, yerel bir olayı damgalar: sayaç artar ve yeni değer döner.
func (l *Lamport) Tick() uint64 {
	return l.c.Add(1)
}

// Observe, uzaktan gelen bir damgayı işler: saat max(yerel, uzak)+1
// olur. Böylece "alım" olayı, "gönderim"den kesinlikle büyük damga alır
// — nedensellik korunur.
func (l *Lamport) Observe(remote uint64) uint64 {
	for {
		cur := l.c.Load()
		next := cur
		if remote > next {
			next = remote
		}
		next++
		if l.c.CompareAndSwap(cur, next) {
			return next
		}
	}
}

// Now, mevcut değeri okur (olay damgalamaz).
func (l *Lamport) Now() uint64 { return l.c.Load() }

// Ordering, iki vector clock'un nedensellik ilişkisidir.
type Ordering int

const (
	Equal      Ordering = iota
	Before              // alıcı: a → b (a, b'nin nedensel geçmişinde)
	After               // b → a
	Concurrent          // ikisi de değil: nedensel bağ yok
)

func (o Ordering) String() string {
	switch o {
	case Before:
		return "before"
	case After:
		return "after"
	case Concurrent:
		return "concurrent"
	default:
		return "equal"
	}
}

// Vector, düğüm-id → sayaç haritasıdır. Görünmeyen düğüm 0 sayılır;
// böylece düğümler sabit bir üyelik listesi olmadan da karşılaştırılır.
// THREAD-SAFE DEĞİLDİR: bir Vector tek bir sürecin/aktörün durumudur
// (actor modelinde doğal: her aktör kendi saatine tek başına dokunur).
type Vector map[string]uint64

// NewVector boş bir saat döner.
func NewVector() Vector { return Vector{} }

// Tick, id'nin bileşenini artırır (yerel olay).
func (v Vector) Tick(id string) uint64 {
	v[id]++
	return v[id]
}

// Merge, uzak saati bileşen bileşen max ile içine alır (mesaj alımında:
// önce Merge, sonra Tick).
func (v Vector) Merge(o Vector) {
	for id, c := range o {
		if c > v[id] {
			v[id] = c
		}
	}
}

// Clone, bağımsız bir kopya döner (mesaja iliştirilecek damga için).
func (v Vector) Clone() Vector {
	out := make(Vector, len(v))
	for id, c := range v {
		out[id] = c
	}
	return out
}

// Compare, v ile o'nun nedensellik ilişkisini döner.
func (v Vector) Compare(o Vector) Ordering {
	var less, greater bool
	for id, vc := range v {
		if oc := o[id]; vc < oc {
			less = true
		} else if vc > oc {
			greater = true
		}
	}
	for id, oc := range o {
		if _, seen := v[id]; !seen && oc > 0 {
			less = true
		}
	}
	switch {
	case less && greater:
		return Concurrent
	case less:
		return Before
	case greater:
		return After
	default:
		return Equal
	}
}
