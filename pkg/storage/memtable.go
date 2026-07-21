package storage

import (
	"bytes"
	"math/rand"
)

// Memtable, yazıların önce düştüğü sıralı in-memory yapıdır. Klasik
// LSM motorları (LevelDB/RocksDB) gibi skip list kullanıyoruz:
// olasılıksal katmanlı bağlı liste — dengeli ağaçların O(log n)
// garantisine rotasyon/yeniden dengeleme olmadan, yalnızca yazı tura
// ile ulaşır. Her düğüm 1/branching olasılıkla bir üst katmana da
// girer; üst katmanlar "ekspres hat" görevi görür.
const (
	skipMaxHeight = 12 // 4^12 >> beklenen memtable kaydı; yeterli
	skipBranching = 4  // katman yükselme olasılığı 1/4 (LevelDB değeri)
)

type skipNode struct {
	rec
	next []*skipNode // next[i] = i. katmandaki sonraki düğüm
}

type memtable struct {
	head *skipNode
	rng  *rand.Rand
	size int // yaklaşık bayt (flush eşiği için)
	n    int // kayıt sayısı
}

func newMemtable() *memtable {
	return &memtable{
		head: &skipNode{next: make([]*skipNode, skipMaxHeight)},
		// Deterministik tohum: testler tekrarlanabilir olsun; memtable
		// yazı kilidi altında tek goroutine'den kullanılır.
		rng: rand.New(rand.NewSource(0x5AD1A2D5)),
	}
}

func (m *memtable) randomHeight() int {
	h := 1
	for h < skipMaxHeight && m.rng.Intn(skipBranching) == 0 {
		h++
	}
	return h
}

// put ekler ya da yerinde günceller (aynı anahtara yeni değer/tombstone).
// Tombstone da normal bir kayıttır: flush'ta diske yazılır ki eski
// SSTable'lardaki değeri gölgeleyebilsin.
func (m *memtable) put(r rec) {
	var prev [skipMaxHeight]*skipNode
	x := m.head
	for lvl := skipMaxHeight - 1; lvl >= 0; lvl-- {
		for x.next[lvl] != nil && bytes.Compare(x.next[lvl].key, r.key) < 0 {
			x = x.next[lvl]
		}
		prev[lvl] = x
	}
	if cand := prev[0].next[0]; cand != nil && bytes.Equal(cand.key, r.key) {
		m.size += len(r.val) - len(cand.val)
		cand.val, cand.tomb = r.val, r.tomb
		return
	}
	h := m.randomHeight()
	node := &skipNode{rec: r, next: make([]*skipNode, h)}
	for lvl := 0; lvl < h; lvl++ {
		node.next[lvl] = prev[lvl].next[lvl]
		prev[lvl].next[lvl] = node
	}
	m.size += len(r.key) + len(r.val) + 16 // +16: kaba düğüm ek yükü
	m.n++
}

func (m *memtable) get(key []byte) (rec, bool) {
	x := m.head
	for lvl := skipMaxHeight - 1; lvl >= 0; lvl-- {
		for x.next[lvl] != nil && bytes.Compare(x.next[lvl].key, key) < 0 {
			x = x.next[lvl]
		}
	}
	if cand := x.next[0]; cand != nil && bytes.Equal(cand.key, key) {
		return cand.rec, true
	}
	return rec{}, false
}

// iter, kayıtları anahtar sırasıyla akıtır (0. katman zaten sıralı
// bağlı listedir). Iterasyon sırasında memtable değişmemelidir; DB
// katmanı bunu kilitle garanti eder.
type memIter struct{ node *skipNode }

func (m *memtable) iter() iterator { return m.iterFrom(nil) }

// iterFrom, from'dan (dahil) itibaren akıtır: skip list'te ilk
// anahtar >= from düğümüne inilir (get ile aynı yürüyüş).
func (m *memtable) iterFrom(from []byte) iterator {
	x := m.head
	for lvl := skipMaxHeight - 1; lvl >= 0; lvl-- {
		for x.next[lvl] != nil && bytes.Compare(x.next[lvl].key, from) < 0 {
			x = x.next[lvl]
		}
	}
	return &memIter{node: x.next[0]}
}

func (it *memIter) next() (rec, bool, error) {
	if it.node == nil {
		return rec{}, false, nil
	}
	r := it.node.rec
	it.node = it.node.next[0]
	return r, true, nil
}
