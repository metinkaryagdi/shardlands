// Package raftstore, pkg/raft'ın Storage arayüzünü pkg/storage (LSM)
// üstünde KALICI olarak uygular. Faz 0'ın iki ayrı yapı taşı burada
// birleşir: konsensüs (Raft) dayanıklılığını, kendi yazdığımız LSM
// motorundan alır.
//
// Neden ayrı paket? pkg/raft yalnızca Storage ARAYÜZÜNÜ tanımlar ve
// hiçbir depolama implementasyonuna bağımlı değildir (Faz 0'da
// MemoryStorage yeterliydi). Kalıcı implementasyonu ayrı pakete koymak
// bu katmanlamayı korur.
//
// Düzen:
//
//	"m"                  → meta (term, votedFor) JSON
//	"e/" + 8B BE index   → log kaydı (term, cmd) JSON
//
// Log'u TEK blob yerine anahtar-başına-kayıt tutmak LSM'in sıralı
// anahtar gücünü kullanır: açılışta aralık taramasıyla (ScanFrom) sırayla
// okunur, yazarken yalnız DEĞİŞEN SONEK yazılır. Save(HardState) tüm
// log'u verdiği için "ne değişti"yi bulmak adına persist edilenin
// in-memory aynası tutulur (Raft zaten log'u bellekte tutuyor; ek maliyet
// yok) ve ilk farklılık noktasından itibaren yazılır — çakışma sonrası
// truncate + append durumunda da doğru.
package raftstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"shardlands/pkg/raft"
	"shardlands/pkg/storage"
)

var (
	metaKey      = []byte("m")
	entryPrefix  = []byte("e/")
	errShortScan = errors.New("raftstore: corrupt entry key")
)

// Store, raft.Storage'ın LSM tabanlı implementasyonudur.
type Store struct {
	db *storage.DB
	// persisted, diske yazdığımız log'un aynası: Save'de ilk farklılık
	// noktasını bulmak için.
	persisted []raft.Entry
}

// Open, dir altında bir raft durum deposu açar. sync=true her yazmada
// fsync yapar: Raft'ın "persist-then-respond" güvenliği için DOĞRU
// seçim (elektrik kesintisinde bile çifte oy imkânsız), bedeli her
// yazmada bir disk turu. sync=false süreç çökmesine dayanır, OS
// çökmesine dayanmaz.
func Open(dir string, sync bool) (*Store, error) {
	db, err := storage.Open(dir, storage.Options{SyncWrites: sync})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	// Aynayı diskteki hâlden kur (restart sonrası doğru diff için).
	hs, err := s.Load()
	if err != nil {
		db.Close()
		return nil, err
	}
	s.persisted = hs.Log
	return s, nil
}

func entryKey(index uint64) []byte {
	k := make([]byte, len(entryPrefix)+8)
	copy(k, entryPrefix)
	binary.BigEndian.PutUint64(k[len(entryPrefix):], index)
	return k
}

type metaRec struct {
	Term     uint64 `json:"t"`
	VotedFor string `json:"v"`
}

type entryRec struct {
	Term uint64 `json:"t"`
	Cmd  []byte `json:"c"`
}

// Save, HardState'i kalıcılaştırır: meta her zaman, log'un yalnız
// değişen soneki. Sıra önemli — önce log kayıtları, sonra meta yazılır;
// meta (term/oy) log'dan geri kalırsa Raft güvenliği bozulmaz (yeniden
// oy istenir), tersi bozardı.
func (s *Store) Save(hs raft.HardState) error {
	// İlk farklılık noktası (0 tabanlı dilim indeksi).
	diff := 0
	for diff < len(s.persisted) && diff < len(hs.Log) {
		a, b := s.persisted[diff], hs.Log[diff]
		if a.Term != b.Term || !bytes.Equal(a.Cmd, b.Cmd) {
			break
		}
		diff++
	}

	// Fazlalık kuyruğu sil (log kısaldıysa veya çakışmayla değiştiyse).
	for i := len(hs.Log); i < len(s.persisted); i++ {
		if err := s.db.Delete(entryKey(uint64(i + 1))); err != nil {
			return err
		}
	}
	// Değişen soneki yaz (Raft index'leri 1 tabanlı).
	for i := diff; i < len(hs.Log); i++ {
		val, err := json.Marshal(entryRec{Term: hs.Log[i].Term, Cmd: hs.Log[i].Cmd})
		if err != nil {
			return err
		}
		if err := s.db.Put(entryKey(uint64(i+1)), val); err != nil {
			return err
		}
	}

	meta, err := json.Marshal(metaRec{Term: hs.Term, VotedFor: hs.VotedFor})
	if err != nil {
		return err
	}
	if err := s.db.Put(metaKey, meta); err != nil {
		return err
	}

	// Aynayı güncelle (kopyala: çağıranın dilimi sonra değişebilir).
	s.persisted = append([]raft.Entry(nil), hs.Log...)
	return nil
}

// Load, diskteki durumu okur (hiç yazılmamışsa sıfır değer).
func (s *Store) Load() (raft.HardState, error) {
	var hs raft.HardState

	val, err := s.db.Get(metaKey)
	switch {
	case err == nil:
		var m metaRec
		if err := json.Unmarshal(val, &m); err != nil {
			return hs, fmt.Errorf("raftstore: meta decode: %w", err)
		}
		hs.Term, hs.VotedFor = m.Term, m.VotedFor
	case errors.Is(err, storage.ErrNotFound):
		// hiç kaydedilmemiş: sıfır durum
	default:
		return hs, err
	}

	// Log kayıtlarını sırayla topla (anahtarlar big-endian → sıralı).
	var scanErr error
	err = s.db.ScanFrom(entryPrefix, func(k, v []byte) bool {
		if !bytes.HasPrefix(k, entryPrefix) {
			return false // prefix bitti
		}
		if len(k) != len(entryPrefix)+8 {
			scanErr = errShortScan
			return false
		}
		idx := binary.BigEndian.Uint64(k[len(entryPrefix):])
		if idx != uint64(len(hs.Log))+1 {
			scanErr = fmt.Errorf("raftstore: log gap at index %d (have %d)", idx, len(hs.Log))
			return false
		}
		var e entryRec
		if err := json.Unmarshal(v, &e); err != nil {
			scanErr = fmt.Errorf("raftstore: entry %d decode: %w", idx, err)
			return false
		}
		hs.Log = append(hs.Log, raft.Entry{Term: e.Term, Cmd: e.Cmd})
		return true
	})
	if scanErr != nil {
		return raft.HardState{}, scanErr
	}
	if err != nil {
		return raft.HardState{}, err
	}
	return hs, nil
}

func (s *Store) Close() error { return s.db.Close() }
