package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// testDir: t.TempDir yerine en-iyi-çaba temizlikli geçici dizin.
// Windows'ta antivirüs, yeni yazılmış/silinmiş dosyaları kısa süre
// delete-pending'de tutabiliyor; t.TempDir'in temizliği bunu test
// hatası sayıyor. Motorun kendi handle disiplini testlerin dosya
// sayısı assert'leriyle zaten doğrulanıyor.
func testDir(tb testing.TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "shardlands-storage-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// crashClose: temiz kapanış YAPMADAN (flush yok, manifest güncellemesi
// yok) dosya tanıtıcılarını kapatır — process crash simülasyonu.
// Windows'ta açık dosyalar silinemediği için tanıtıcıları kapatmak
// şart; gerçek bir crash'te OS bunu bizim yerimize yapardı.
func (db *DB) crashClose() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	db.wal.f.Close()
	db.closeTables()
}

func mustOpen(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustAbsent(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) = %v, want ErrNotFound", key, err)
	}
}

func TestPutGetDeleteInMemory(t *testing.T) {
	db := mustOpen(t, testDir(t), Options{})
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1")
	if err := db.Put([]byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "2")
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	mustAbsent(t, db, "a")
	mustAbsent(t, db, "yok")
	if err := db.Put(nil, []byte("v")); err == nil {
		t.Fatal("empty key must be rejected")
	}
}

// Flush sonrası: tablo okunmalı; memtable'daki yeni yazma tabloyu,
// yeni tablodaki tombstone eski tabloyu gölgelemeli.
func TestFlushAndShadowing(t *testing.T) {
	db := mustOpen(t, testDir(t), Options{})
	defer db.Close()

	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("stays"), []byte("here"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if db.TableCount() != 1 {
		t.Fatalf("tables = %d, want 1", db.TableCount())
	}
	mustGet(t, db, "k", "v1") // tablodan

	db.Put([]byte("k"), []byte("v2"))
	mustGet(t, db, "k", "v2") // memtable tabloyu gölgeler

	db.Delete([]byte("k"))
	db.Flush() // tombstone artık en yeni tabloda
	mustAbsent(t, db, "k")
	mustGet(t, db, "stays", "here")
}

func TestReopenPersistence(t *testing.T) {
	dir := testDir(t)
	db := mustOpen(t, dir, Options{})
	for i := 0; i < 50; i++ {
		db.Put([]byte(fmt.Sprintf("k-%02d", i)), []byte(fmt.Sprintf("v-%d", i)))
	}
	db.Delete([]byte("k-07"))
	if err := db.Close(); err != nil { // Close flush eder
		t.Fatal(err)
	}

	db2 := mustOpen(t, dir, Options{})
	defer db2.Close()
	mustGet(t, db2, "k-00", "v-0")
	mustGet(t, db2, "k-49", "v-49")
	mustAbsent(t, db2, "k-07")
}

// Otomatik flush + otomatik compaction: minik memtable her Put'ta
// flush, CompactAt=3'te birleşme → dosya sayısı sınırlı kalmalı, veri
// eksiksiz olmalı.
func TestAutoFlushAndCompact(t *testing.T) {
	db := mustOpen(t, testDir(t), Options{MemtableBytes: 1, CompactAt: 3})
	defer db.Close()

	for i := 0; i < 10; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k-%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if n := db.TableCount(); n >= 3 {
		t.Fatalf("tables = %d, want < 3 (auto-compaction must bound file count)", n)
	}
	for i := 0; i < 10; i++ {
		mustGet(t, db, fmt.Sprintf("k-%02d", i), "v")
	}
}

// Compaction: tombstone'lar düşmeli, tek dosya kalmalı ve silinen
// anahtar reopen'dan sonra da geri DİRİLMEMELİ (resurrection kontrolü).
func TestCompactionDropsTombstonesNoResurrection(t *testing.T) {
	dir := testDir(t)
	db := mustOpen(t, dir, Options{})

	db.Put([]byte("dead"), []byte("old-value"))
	db.Put([]byte("alive"), []byte("keep"))
	db.Flush()
	db.Delete([]byte("dead"))
	db.Flush()
	if db.TableCount() != 2 {
		t.Fatalf("tables = %d, want 2", db.TableCount())
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if db.TableCount() != 1 {
		t.Fatalf("tables after compact = %d, want 1", db.TableCount())
	}
	mustAbsent(t, db, "dead")
	mustGet(t, db, "alive", "keep")

	// Diskte de tek .sst kalmalı (eskiler gerçekten silindi).
	matches, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(matches) != 1 {
		t.Fatalf("on-disk .sst count = %d, want 1", len(matches))
	}

	db.Close()
	db2 := mustOpen(t, dir, Options{})
	defer db2.Close()
	mustAbsent(t, db2, "dead") // eski değer dirilmemeli
	mustGet(t, db2, "alive", "keep")
}

// Chaos: flush edilmemiş yazılar crash'ten sonra WAL replay ile geri
// gelmeli.
func TestCrashRecoveryFromWAL(t *testing.T) {
	dir := testDir(t)
	db := mustOpen(t, dir, Options{})
	db.Put([]byte("flushed"), []byte("on-disk"))
	db.Flush()
	db.Put([]byte("unflushed"), []byte("only-in-wal"))
	db.Delete([]byte("flushed"))
	db.crashClose() // flush YOK

	db2 := mustOpen(t, dir, Options{})
	defer db2.Close()
	mustGet(t, db2, "unflushed", "only-in-wal")
	mustAbsent(t, db2, "flushed") // tombstone da WAL'daydı
}

// Chaos: WAL kuyruğunda yarım kayıt (torn write) — öncesi kurtarılmalı,
// yarım kayıt sessizce atılmalı.
func TestTornWALTail(t *testing.T) {
	dir := testDir(t)
	db := mustOpen(t, dir, Options{})
	db.Put([]byte("good-1"), []byte("v1"))
	db.Put([]byte("good-2"), []byte("v2"))
	db.crashClose()

	// Crash anında yarım kalmış kayıt simülasyonu: uzunluk öneki 100
	// bayt vaat ediyor, yalnızca 3 bayt var.
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{100, 0xAB, 0xCD, 0xEF})
	f.Close()

	db2 := mustOpen(t, dir, Options{})
	defer db2.Close()
	mustGet(t, db2, "good-1", "v1")
	mustGet(t, db2, "good-2", "v2")
}

// Chaos: manifest'e girmemiş .sst (flush ortasında crash) açılışta
// temizlenmeli ve veri WAL'dan gelmeli.
func TestOrphanTableCleanedUp(t *testing.T) {
	dir := testDir(t)
	db := mustOpen(t, dir, Options{})
	db.Put([]byte("k"), []byte("v"))
	db.crashClose()

	// flushLocked'ın manifest'ten önce crash etmiş hali: dosya var,
	// manifest onu bilmiyor.
	orphan := filepath.Join(dir, "000099.sst")
	if err := writeTable(orphan, &sliceIter{recs: []rec{{key: []byte("zombi"), val: []byte("x")}}}, 10); err != nil {
		t.Fatal(err)
	}

	db2 := mustOpen(t, dir, Options{})
	defer db2.Close()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan .sst must be deleted on open")
	}
	mustAbsent(t, db2, "zombi") // yarım dosyadan veri sızmamalı
	mustGet(t, db2, "k", "v")   // WAL replay çalışmalı
}

func TestScanMergedAndOrdered(t *testing.T) {
	db := mustOpen(t, testDir(t), Options{})
	defer db.Close()

	db.Put([]byte("b"), []byte("from-table"))
	db.Put([]byte("d"), []byte("dead"))
	db.Flush()
	db.Put([]byte("a"), []byte("from-mem"))
	db.Put([]byte("b"), []byte("overwritten")) // memtable tabloyu gölgeler
	db.Delete([]byte("d"))                     // tombstone Scan'de görünmemeli

	var keys, vals []string
	err := db.Scan(func(k, v []byte) bool {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"a", "b"}
	wantVals := []string{"from-mem", "overwritten"}
	if fmt.Sprint(keys) != fmt.Sprint(wantKeys) || fmt.Sprint(vals) != fmt.Sprint(wantVals) {
		t.Fatalf("scan = %v/%v, want %v/%v", keys, vals, wantKeys, wantVals)
	}
}

// ScanFrom: memtable + birden çok tablo + tombstone karışımında, verilen
// anahtardan itibaren sıralı ve eksiksiz akmalı.
func TestScanFromMidRange(t *testing.T) {
	db := mustOpen(t, testDir(t), Options{})
	defer db.Close()

	// k00..k29 tabloda, k30..k59 ikinci tabloda, k60..k89 memtable'da.
	for i := 0; i < 90; i++ {
		db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i)))
		if i == 29 || i == 59 {
			db.Flush()
		}
	}
	db.Delete([]byte("k45")) // ortada bir tombstone

	var got []string
	if err := db.ScanFrom([]byte("k40"), func(k, v []byte) bool {
		got = append(got, string(k))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	want := 90 - 40 - 1 // k40'tan sona, k45 silinmiş
	if len(got) != want {
		t.Fatalf("scanned %d keys, want %d (got: %v...)", len(got), want, got[:min(5, len(got))])
	}
	if got[0] != "k40" || got[len(got)-1] != "k89" {
		t.Fatalf("range = [%s..%s], want [k40..k89]", got[0], got[len(got)-1])
	}
	for _, k := range got {
		if k == "k45" {
			t.Fatal("tombstoned key leaked into scan")
		}
	}

	// Sınırlar: tam bir dilim başlangıcına ve son anahtara denk gelen from.
	got = got[:0]
	db.ScanFrom([]byte("k89"), func(k, v []byte) bool { got = append(got, string(k)); return true })
	if len(got) != 1 || got[0] != "k89" {
		t.Fatalf("from=last: %v, want [k89]", got)
	}
	got = got[:0]
	db.ScanFrom([]byte("z"), func(k, v []byte) bool { got = append(got, string(k)); return true })
	if len(got) != 0 {
		t.Fatalf("from beyond end: %v, want empty", got)
	}
}

func TestMergeIterNewestWins(t *testing.T) {
	newer := &sliceIter{recs: []rec{
		{key: []byte("a"), val: []byte("new-a")},
		{key: []byte("c"), tomb: true},
	}}
	older := &sliceIter{recs: []rec{
		{key: []byte("a"), val: []byte("old-a")},
		{key: []byte("b"), val: []byte("old-b")},
		{key: []byte("c"), val: []byte("old-c")},
	}}
	it := newMergeIter([]iterator{newer, older})

	var got []string
	for {
		r, ok, err := it.next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, fmt.Sprintf("%s=%s/%v", r.key, r.val, r.tomb))
	}
	want := []string{"a=new-a/false", "b=old-b/false", "c=/true"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("merge = %v, want %v", got, want)
	}
}

func BenchmarkPut(b *testing.B) {
	db, err := Open(testDir(b), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	key := make([]byte, 16)
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(key, fmt.Sprintf("key-%012d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db, err := Open(testDir(b), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	const n = 100000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key-%06d", i)), []byte("value"))
	}
	db.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key-%06d", i%n))); err != nil {
			b.Fatal(err)
		}
	}
}
