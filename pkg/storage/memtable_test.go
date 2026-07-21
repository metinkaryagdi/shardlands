package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// Rastgele put/overwrite/tombstone dizisi, referans map ile birebir
// aynı sonucu vermeli; iterasyon sıralı ve eksiksiz olmalı.
func TestMemtableAgainstReferenceMap(t *testing.T) {
	m := newMemtable()
	type state struct {
		val  string
		tomb bool
	}
	ref := map[string]state{}
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key-%04d", rng.Intn(800))
		if rng.Intn(3) == 2 {
			m.put(rec{key: []byte(k), tomb: true})
			ref[k] = state{tomb: true}
		} else {
			v := fmt.Sprintf("val-%d", i)
			m.put(rec{key: []byte(k), val: []byte(v)})
			ref[k] = state{val: v}
		}
	}

	// Nokta sorguları referansla eşleşmeli.
	for k, want := range ref {
		got, ok := m.get([]byte(k))
		if !ok {
			t.Fatalf("get(%q): missing", k)
		}
		if got.tomb != want.tomb || string(got.val) != want.val {
			t.Fatalf("get(%q) = (%q,%v), want (%q,%v)",
				k, got.val, got.tomb, want.val, want.tomb)
		}
	}
	if _, ok := m.get([]byte("hiç-yok")); ok {
		t.Fatal("get on absent key must return false")
	}

	// İterasyon: kesin artan sırada ve ref ile aynı kayıt kümesi.
	it := m.iter()
	var lastKey []byte
	seen := 0
	for {
		r, ok, err := it.next()
		if err != nil {
			t.Fatalf("iter error: %v", err)
		}
		if !ok {
			break
		}
		if lastKey != nil && bytes.Compare(r.key, lastKey) <= 0 {
			t.Fatalf("iteration not strictly ascending: %q after %q", r.key, lastKey)
		}
		lastKey = append(lastKey[:0], r.key...)
		want, exists := ref[string(r.key)]
		if !exists || want.tomb != r.tomb || want.val != string(r.val) {
			t.Fatalf("iter yielded %q=(%q,%v), ref says (%q,%v,%v)",
				r.key, r.val, r.tomb, want.val, want.tomb, exists)
		}
		seen++
	}
	if seen != len(ref) {
		t.Fatalf("iterated %d records, want %d", seen, len(ref))
	}
	if m.n != len(ref) {
		t.Fatalf("m.n = %d, want %d", m.n, len(ref))
	}
	if m.size <= 0 {
		t.Fatalf("size = %d, want > 0", m.size)
	}
}

// Aynı anahtara üst üste yazmak kayıt sayısını artırmamalı, boyutu
// makul tutmalı (yerinde güncelleme).
func TestMemtableOverwriteInPlace(t *testing.T) {
	m := newMemtable()
	for i := 0; i < 100; i++ {
		m.put(rec{key: []byte("k"), val: []byte(fmt.Sprintf("v-%d", i))})
	}
	if m.n != 1 {
		t.Fatalf("n = %d, want 1 (in-place update)", m.n)
	}
	got, ok := m.get([]byte("k"))
	if !ok || string(got.val) != "v-99" {
		t.Fatalf("get = (%q,%v), want (v-99,true)", got.val, ok)
	}
}
