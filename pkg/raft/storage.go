package raft

import "sync"

// HardState, crash'te KAYBOLMAMASI gereken durumdur: currentTerm ve
// votedFor kaybolursa düğüm aynı dönemde ikinci kez oy verebilir (iki
// lider!); log kaybolursa commit edilmiş veri uçar. Geri kalan her şey
// (commitIndex, nextIndex...) yeniden türetilebilir.
type HardState struct {
	Term     uint64
	VotedFor string
	Log      []Entry
}

// Storage, HardState'in kalıcı arşividir. Node her kritik değişimde
// Save çağırır ve BAŞARIYI BEKLER (persist-then-respond). Bu
// implementasyonda tüm state her seferinde yazılır (basit, O(log));
// gerçek motor append-only WAL kullanır — Faz 3'te pkg/storage
// tabanlı bir implementasyon takılabilir.
type Storage interface {
	Save(HardState) error
	Load() (HardState, error)
}

// MemoryStorage: RAM'de yaşayan Storage — testlerde "crash ama disk
// sağlam" senaryosu için aynı instance yeni Node'a verilir.
type MemoryStorage struct {
	mu sync.Mutex
	hs HardState
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Save(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Log dilimini KOPYALA: Node truncate sonrası aynı backing array'e
	// append edebilir; paylaşılan dilim burada sessizce bozulurdu.
	// (Cmd baytları sözleşme gereği değişmez, paylaşılabilir.)
	m.hs = HardState{
		Term:     hs.Term,
		VotedFor: hs.VotedFor,
		Log:      append([]Entry(nil), hs.Log...),
	}
	return nil
}

func (m *MemoryStorage) Load() (HardState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return HardState{
		Term:     m.hs.Term,
		VotedFor: m.hs.VotedFor,
		Log:      append([]Entry(nil), m.hs.Log...),
	}, nil
}
