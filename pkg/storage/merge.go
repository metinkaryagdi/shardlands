package storage

import "bytes"

// mergeIter, birden çok sıralı kaynağı tek sıralı akışta birleştirir.
// Kaynak sırası ÖNCELİKTİR: srcs[0] en yeni. Aynı anahtar birden çok
// kaynakta varsa en yeni kaynağın kaydı kazanır, eskiler sessizce
// atlanır — LSM'nin "gölgeleme" kuralının kendisi. Compaction ve Scan
// aynı mekanizmayı kullanır.
//
// Kaynak sayımız küçük (memtable + birkaç tablo) olduğu için min-heap
// yerine doğrusal tarama yeterli; N kaynak için O(N) seçim maliyeti
// pratikte heap sabitinden ucuz.
type mergeIter struct {
	srcs  []iterator
	heads []*rec // her kaynağın bekleyen kaydı; nil = çekilmeli
	done  []bool
}

func newMergeIter(srcs []iterator) *mergeIter {
	return &mergeIter{
		srcs:  srcs,
		heads: make([]*rec, len(srcs)),
		done:  make([]bool, len(srcs)),
	}
}

func (m *mergeIter) next() (rec, bool, error) {
	// Boş başları doldur.
	for i := range m.srcs {
		if m.heads[i] == nil && !m.done[i] {
			r, ok, err := m.srcs[i].next()
			if err != nil {
				return rec{}, false, err
			}
			if !ok {
				m.done[i] = true
				continue
			}
			m.heads[i] = &r
		}
	}
	// En küçük anahtar; eşitlikte en düşük indeks (en yeni) kazanır.
	best := -1
	for i, h := range m.heads {
		if h == nil {
			continue
		}
		if best == -1 || bytes.Compare(h.key, m.heads[best].key) < 0 {
			best = i
		}
	}
	if best == -1 {
		return rec{}, false, nil
	}
	out := *m.heads[best]
	// Aynı anahtarı taşıyan TÜM başları tüket (eskiler gölgede kaldı).
	for i, h := range m.heads {
		if h != nil && bytes.Equal(h.key, out.key) {
			m.heads[i] = nil
		}
	}
	return out, true, nil
}

// dropTombIter, akıştan tombstone'ları süzer. Compaction TÜM tabloları
// birleştirirken kullanılır: geride gölgelenecek daha eski katman
// kalmadığı için "silindi" işaretini taşımaya gerek yoktur. Kısmi
// compaction yapılsaydı tombstone atmak eski değeri "diriltirdi" —
// bu yüzden filtre yalnızca tam birleştirmede devreye girer.
type dropTombIter struct{ src iterator }

func (d *dropTombIter) next() (rec, bool, error) {
	for {
		r, ok, err := d.src.next()
		if err != nil || !ok {
			return rec{}, ok, err
		}
		if !r.tomb {
			return r, true, nil
		}
	}
}
