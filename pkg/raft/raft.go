// Package raft, Raft konsensüs algoritmasının (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm") sıfırdan
// implementasyonudur. Faz 3'te shard lideri seçimi ve replikasyon için
// kullanılacak.
//
// Raft'ın üç parçası: leader election (randomize zaman aşımı + çoğunluk
// oyu), log replication (lider log'u dayatır, çoğunluk onayı = commit)
// ve safety (yalnızca güncel log'lu aday oy alır; lider yalnız KENDİ
// dönemindeki kaydı çoğunlukla commit sayar — §5.4.2).
//
// Tasarım notları:
//   - RPC'ler Transport arayüzünün arkasında; testler partition simüle
//     eden in-memory Network kullanır, gerçek ağ (gRPC) Faz 3'te takılır.
//   - currentTerm/votedFor/log her değişimde Storage'a yazılır (persist
//     ÖNCE, cevap SONRA — aksi halde restart sonrası aynı dönemde iki
//     kez oy verilebilir).
//   - Snapshot/log compaction ve üyelik değişikliği bilinçli olarak
//     kapsam dışı (README'de tartışılıyor).
package raft

import "time"

// State, düğümün roldür.
type State int32

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// Entry, replikasyon log'unun bir kaydıdır. Index'ler 1'den başlar;
// Term, kaydın hangi liderin döneminde yazıldığını söyler (çakışma
// tespitinin anahtarı).
type Entry struct {
	Term uint64
	Cmd  []byte
}

// ApplyMsg, commit edilmiş bir kaydın state machine'e teslimi.
type ApplyMsg struct {
	Index uint64
	Term  uint64
	Cmd   []byte
}

// RequestVote RPC'si (§5.2).
type RequestVoteReq struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResp struct {
	Term    uint64
	Granted bool
}

// AppendEntries RPC'si (§5.3); boş Entries = heartbeat.
type AppendEntriesReq struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResp struct {
	Term    uint64
	Success bool
}

// Config, bir düğümün kimliği ve bağımlılıkları.
type Config struct {
	ID    string
	Peers []string // diğer düğümlerin id'leri (kendisi hariç)

	Transport Transport
	Storage   Storage        // nil ise NewMemoryStorage()
	Apply     func(ApplyMsg) // commit edilen kayıtlar sırayla buraya; nil = yok say

	// Zamanlama. Kural: HeartbeatInterval << ElectionTimeoutMin, yoksa
	// sağlıklı liderler bile devrilir. Randomize aralık [Min, Max] split
	// vote'ları kırar: aynı anda uyanan adaylar oyları bölüşüp turu
	// boşa harcar; farklı zamanlarda uyananlarda ilk uyanan kazanır.
	ElectionTimeoutMin time.Duration // varsayılan 150ms
	ElectionTimeoutMax time.Duration // varsayılan 300ms
	HeartbeatInterval  time.Duration // varsayılan 50ms
	TickInterval       time.Duration // varsayılan 10ms
}

func (c *Config) withDefaults() {
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.TickInterval <= 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.Storage == nil {
		c.Storage = NewMemoryStorage()
	}
	if c.Apply == nil {
		c.Apply = func(ApplyMsg) {}
	}
}
