package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	manifestName = "MANIFEST"
	walName      = "wal.log"
)

// Options, DB davranışını ayarlar; sıfır değerler makul varsayılanlara
// çekilir.
type Options struct {
	// MemtableBytes: memtable bu boyutu aşınca SSTable'a flush edilir
	// (varsayılan 4 MiB).
	MemtableBytes int
	// CompactAt: SSTable sayısı bu eşiğe ulaşınca hepsi tek tabloya
	// birleştirilir (varsayılan 4).
	CompactAt int
	// SyncWrites: her WAL kaydında fsync (bkz. wal.go trade-off notu).
	SyncWrites bool
	// BloomBitsPerKey: SSTable bloom filtresi yoğunluğu (varsayılan 10).
	BloomBitsPerKey int
}

func (o *Options) withDefaults() {
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = 4 << 20
	}
	if o.CompactAt <= 0 {
		o.CompactAt = 4
	}
	if o.BloomBitsPerKey <= 0 {
		o.BloomBitsPerKey = 10
	}
}

// DB, LSM motorunun dış yüzüdür. Eşzamanlılık modeli kaba ama net:
// yazılar (Put/Delete/Flush/Compact) tek yazar kilidi altında, okumalar
// (Get/Scan) paylaşımlı kilit altında. MVCC (kilitsiz snapshot okuma)
// Faz 2'nin konusu.
type DB struct {
	mu     sync.RWMutex
	dir    string
	opts   Options
	mem    *memtable
	wal    *wal
	tables []*sstable // [0] en yeni
	nextID uint64
	closed bool
}

// Open, dir altındaki veriyi yükler (yoksa yaratır): MANIFEST'teki
// SSTable'lar açılır, MANIFEST'te olmayan .sst dosyaları (crash artığı
// yarım dosyalar) silinir, WAL replay ile memtable kurulur.
func Open(dir string, opts Options) (*DB, error) {
	opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, opts: opts, mem: newMemtable(), nextID: 1}

	names, err := readManifest(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for _, name := range names {
		t, err := openTable(filepath.Join(dir, name))
		if err != nil {
			db.closeTables()
			return nil, fmt.Errorf("manifest table %s: %w", name, err)
		}
		db.tables = append(db.tables, t)
		live[name] = true
		if id := tableID(name); id >= db.nextID {
			db.nextID = id + 1
		}
	}

	// Manifest'e girmemiş .sst = tamamlanmadan crash olmuş yazma; güvenle sil.
	entries, err := os.ReadDir(dir)
	if err != nil {
		db.closeTables()
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".sst") && !live[name] {
			os.Remove(filepath.Join(dir, name))
			if id := tableID(name); id >= db.nextID {
				db.nextID = id + 1 // silinen numarayı yine de yakma
			}
		}
	}

	walPath := filepath.Join(dir, walName)
	if err := replayWAL(walPath, db.mem.put); err != nil {
		db.closeTables()
		return nil, err
	}
	w, err := openWAL(walPath, opts.SyncWrites)
	if err != nil {
		db.closeTables()
		return nil, err
	}
	db.wal = w
	return db, nil
}

func (db *DB) Put(key, val []byte) error {
	return db.write(rec{key: clone(key), val: clone(val)})
}

// Delete bir tombstone yazar; anahtar hiç yoksa bile geçerlidir (eski
// bir SSTable'da olup olmadığını bilemeyiz, işaret bırakmak zorundayız).
func (db *DB) Delete(key []byte) error {
	return db.write(rec{key: clone(key), tomb: true})
}

func (db *DB) write(r rec) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if len(r.key) == 0 {
		return fmt.Errorf("storage: empty key")
	}
	// Önce WAL (dayanıklılık), sonra memtable (görünürlük). Ters sıra
	// crash'te "okunabilmiş ama log'lanmamış" yazma üretirdi.
	if err := db.wal.append(r); err != nil {
		return err
	}
	db.mem.put(r)
	if db.mem.size >= db.opts.MemtableBytes {
		if err := db.flushLocked(); err != nil {
			return err
		}
		if len(db.tables) >= db.opts.CompactAt {
			return db.compactLocked()
		}
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	// Yeniden eskiye: memtable → tablolar. İlk bulunan kayıt kazanır;
	// tombstone ise anahtar "silinmiş"tir, daha eskiye BAKILMAZ.
	if r, ok := db.mem.get(key); ok {
		if r.tomb {
			return nil, ErrNotFound
		}
		return clone(r.val), nil
	}
	for _, t := range db.tables {
		r, ok, err := t.get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			if r.tomb {
				return nil, ErrNotFound
			}
			return clone(r.val), nil
		}
	}
	return nil, ErrNotFound
}

// Scan, canlı kayıtları (tombstone'suz birleşik görünüm) anahtar
// sırasıyla fn'e verir; fn false dönerse durur.
func (db *DB) Scan(fn func(key, val []byte) bool) error {
	return db.ScanFrom(nil, fn)
}

// ScanFrom, from anahtarından (dahil) itibaren tarar. Kilidi tarama
// boyunca tutar — uzun taramalar yazıları bekletir (MVCC gelince
// kalkacak).
func (db *DB) ScanFrom(from []byte, fn func(key, val []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	srcs := []iterator{db.mem.iterFrom(from)}
	for _, t := range db.tables {
		srcs = append(srcs, t.iterFrom(from))
	}
	it := &dropTombIter{src: newMergeIter(srcs)}
	for {
		r, ok, err := it.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !fn(r.key, r.val) {
			return nil
		}
	}
}

// Flush, memtable'ı elle SSTable'a döker (testler ve kontrollü kapanış).
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.flushLocked()
}

// Compact, tüm SSTable'ları tek tabloya birleştirir ve tombstone'ları
// düşürür.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.compactLocked()
}

// Close, memtable'ı flush edip dosyaları kapatır (temiz kapanış).
// Flush başarısız olsa bile dosya tanıtıcıları MUTLAKA kapatılır —
// yarı kapalı bir DB, dizini silinemez halde bırakır (özellikle
// Windows'ta) ve hatayı gizler.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	flushErr := db.flushLocked()
	walErr := db.wal.close()
	db.closeTables()
	if flushErr != nil {
		return flushErr
	}
	return walErr
}

// ---- iç işleyiş ----

// flushLocked: memtable → yeni SSTable. Sıra kritik: (1) tablo yaz +
// fsync, (2) MANIFEST'i atomik güncelle (dosya artık "var"), (3) WAL
// sıfırla, (4) taze memtable. (2)'den önce crash → dosya manifest'te
// yok, açılışta silinir, WAL hâlâ dolu → veri kaybı yok. (3)'ten önce
// crash → hem tabloda hem WAL'da aynı kayıtlar → replay aynı değerleri
// üstüne yazar, idempotent.
func (db *DB) flushLocked() error {
	if db.mem.n == 0 {
		return nil
	}
	name := fmt.Sprintf("%06d.sst", db.nextID)
	path := filepath.Join(db.dir, name)
	if err := writeTable(path, db.mem.iter(), db.opts.BloomBitsPerKey); err != nil {
		return err
	}
	t, err := openTable(path)
	if err != nil {
		return err
	}
	db.nextID++
	db.tables = append([]*sstable{t}, db.tables...)
	if err := db.writeManifestLocked(); err != nil {
		return err
	}
	if err := db.wal.reset(); err != nil {
		return err
	}
	db.mem = newMemtable()
	return nil
}

// compactLocked: tüm tabloları tek tabloya birleştirir. TÜM tablolar
// birleştiği için tombstone düşürmek güvenli (gölgelenecek eski katman
// kalmıyor; bkz. dropTombIter). Eski dosyalar ancak yeni tablo
// MANIFEST'e girdikten sonra silinir — arada crash olursa açılış ya
// eski manifest'i (eski dosyalar) ya yenisini (yeni dosya) görür, iki
// dünya da tutarlıdır.
func (db *DB) compactLocked() error {
	if len(db.tables) < 2 {
		return nil
	}
	srcs := make([]iterator, len(db.tables))
	for i, t := range db.tables {
		srcs[i] = t.iter()
	}
	name := fmt.Sprintf("%06d.sst", db.nextID)
	path := filepath.Join(db.dir, name)
	if err := writeTable(path, &dropTombIter{src: newMergeIter(srcs)}, db.opts.BloomBitsPerKey); err != nil {
		return err
	}
	t, err := openTable(path)
	if err != nil {
		return err
	}
	db.nextID++
	old := db.tables
	db.tables = []*sstable{t}
	if err := db.writeManifestLocked(); err != nil {
		return err
	}
	for _, o := range old {
		o.close() // Windows: açık dosya silinemez, önce kapat
		os.Remove(o.path)
	}
	return nil
}

// writeManifestLocked: canlı tablo listesi (yeniden eskiye) temp dosyaya
// yazılıp fsync + rename ile atomik değiştirilir. MANIFEST tek doğruluk
// kaynağıdır: bir .sst ancak burada listeleniyorsa "var"dır.
func (db *DB) writeManifestLocked() error {
	tmp := filepath.Join(db.dir, manifestName+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, t := range db.tables {
		if _, err := fmt.Fprintln(f, filepath.Base(t.path)); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(db.dir, manifestName))
}

func readManifest(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			names = append(names, line)
		}
	}
	return names, nil
}

func tableID(name string) uint64 {
	id, _ := strconv.ParseUint(strings.TrimSuffix(name, ".sst"), 10, 64)
	return id
}

func (db *DB) closeTables() {
	for _, t := range db.tables {
		t.close()
	}
}

// TableCount: canlı SSTable sayısı (testler ve gözlemlenebilirlik).
func (db *DB) TableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.tables)
}
