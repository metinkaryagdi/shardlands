// Package storage, Shardlands için sıfırdan yazılmış LSM-tree tabanlı
// bir key-value storage engine'dir. Faz 2'de event store olarak
// kullanılacak.
//
// Yazı yolu: Put/Delete önce WAL'a (dayanıklılık), sonra memtable'a
// (sıralı in-memory skip list) düşer. Memtable dolunca değişmez bir
// SSTable dosyasına "flush" edilir. Okuma yolu: memtable → SSTable'lar
// (yeniden eskiye). Silme, bir tombstone kaydıdır; gerçek temizlik
// compaction'da olur. SSTable sayısı arttıkça compaction hepsini tek
// dosyada birleştirir ve tombstone'ları düşürür.
package storage

import "errors"

var (
	// ErrNotFound: anahtar yok (veya silinmiş).
	ErrNotFound = errors.New("storage: key not found")
	// ErrClosed: kapatılmış DB üzerinde işlem.
	ErrClosed = errors.New("storage: db is closed")
	// ErrCorrupt: CRC/format doğrulaması başarısız.
	ErrCorrupt = errors.New("storage: corrupt data")
)

// rec, motorun her katmanında (memtable, WAL, SSTable, merge) dolaşan
// tek kayıt biçimidir. tomb=true bir silme işaretidir (tombstone):
// değeri "yok" değil "silindi" — eski katmanlardaki değeri gölgeler.
type rec struct {
	key  []byte
	val  []byte
	tomb bool
}

// iterator, anahtar sırasına göre kayıt akıtan her kaynağın ortak
// arayüzüdür (memtable, SSTable, merge). Kaynak tükenince ok=false.
type iterator interface {
	next() (rec, bool, error)
}

func clone(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
