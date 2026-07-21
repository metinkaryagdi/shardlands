package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// sliceIter: testlerde hazır kayıt dilimini iterator olarak akıtmak için.
type sliceIter struct {
	recs []rec
	i    int
}

func (s *sliceIter) next() (rec, bool, error) {
	if s.i >= len(s.recs) {
		return rec{}, false, nil
	}
	r := s.recs[s.i]
	s.i++
	return r, true, nil
}

func buildTestTable(t *testing.T, recs []rec) *sstable {
	t.Helper()
	path := filepath.Join(testDir(t), "test.sst")
	if err := writeTable(path, &sliceIter{recs: recs}, 10); err != nil {
		t.Fatalf("writeTable: %v", err)
	}
	tbl, err := openTable(path)
	if err != nil {
		t.Fatalf("openTable: %v", err)
	}
	t.Cleanup(func() { tbl.close() })
	return tbl
}

// Yazılan her kayıt (tombstone dahil) get ve iterasyonla birebir geri
// okunmalı; sparse index sınırları (ilk/son/dilim kenarları) dahil.
func TestSSTableRoundTrip(t *testing.T) {
	// 100 kayıt: indexInterval'ın (16) katlarına ve komşularına denk
	// gelen anahtarlar dilim sınırlarını da test eder.
	var recs []rec
	for i := 0; i < 100; i++ {
		r := rec{key: []byte(fmt.Sprintf("key-%03d", i))}
		if i%7 == 0 {
			r.tomb = true
		} else {
			r.val = []byte(fmt.Sprintf("val-%d", i))
		}
		recs = append(recs, r)
	}
	tbl := buildTestTable(t, recs)

	for _, want := range recs {
		got, ok, err := tbl.get(want.key)
		if err != nil {
			t.Fatalf("get(%q): %v", want.key, err)
		}
		if !ok {
			t.Fatalf("get(%q): missing", want.key)
		}
		if got.tomb != want.tomb || !bytes.Equal(got.val, want.val) {
			t.Fatalf("get(%q) = (%q,%v), want (%q,%v)", want.key, got.val, got.tomb, want.val, want.tomb)
		}
	}

	// Olmayan anahtarlar: en küçükten küçük, aralarda, en büyükten büyük.
	for _, k := range []string{"aaa", "key-0005x", "key-050x", "zzz"} {
		if _, ok, err := tbl.get([]byte(k)); err != nil || ok {
			t.Fatalf("get(%q) = ok=%v err=%v, want absent", k, ok, err)
		}
	}

	it := tbl.iter()
	for i := 0; ; i++ {
		r, ok, err := it.next()
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if !ok {
			if i != len(recs) {
				t.Fatalf("iterated %d, want %d", i, len(recs))
			}
			break
		}
		if !bytes.Equal(r.key, recs[i].key) || !bytes.Equal(r.val, recs[i].val) || r.tomb != recs[i].tomb {
			t.Fatalf("iter[%d] = %q, want %q", i, r.key, recs[i].key)
		}
	}
}

func TestSSTableRejectsUnsortedInput(t *testing.T) {
	path := filepath.Join(testDir(t), "bad.sst")
	recs := []rec{
		{key: []byte("b"), val: []byte("1")},
		{key: []byte("a"), val: []byte("2")},
	}
	if err := writeTable(path, &sliceIter{recs: recs}, 10); err == nil {
		t.Fatal("unsorted input must be rejected")
	}
}

// Veri bölgesinde bit çürümesi CRC ile yakalanmalı.
func TestSSTableCorruptionDetected(t *testing.T) {
	dir := testDir(t)
	path := filepath.Join(dir, "c.sst")
	recs := []rec{{key: []byte("hello"), val: []byte("world")}}
	if err := writeTable(path, &sliceIter{recs: recs}, 10); err != nil {
		t.Fatalf("writeTable: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[8] ^= 0xFF // veri bölgesinde bir bayt boz
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	tbl, err := openTable(path) // footer/index sağlam: açılış başarılı
	if err != nil {
		t.Fatalf("openTable: %v", err)
	}
	defer tbl.close()
	if _, _, err := tbl.get([]byte("hello")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("get on corrupt entry = %v, want ErrCorrupt", err)
	}
}

// Footer'ı bozuk (kesik) dosya açılışta reddedilmeli.
func TestSSTableBadFooter(t *testing.T) {
	path := filepath.Join(testDir(t), "trunc.sst")
	if err := os.WriteFile(path, []byte("çok kısa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openTable(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("openTable = %v, want ErrCorrupt", err)
	}
}

// Boş tablo (compaction her şeyi düşürdüyse) geçerli olmalı.
func TestSSTableEmpty(t *testing.T) {
	tbl := buildTestTable(t, nil)
	if _, ok, err := tbl.get([]byte("x")); err != nil || ok {
		t.Fatalf("get on empty = ok=%v err=%v", ok, err)
	}
	if _, ok, err := tbl.iter().next(); err != nil || ok {
		t.Fatalf("iter on empty = ok=%v err=%v", ok, err)
	}
}
