// Package dlock, kendi Raft implementasyonumuz (pkg/raft) üstünde
// LEASE TABANLI dağıtık kilit sağlar.
//
// Neden Raft üstünde? Dağıtık kilit, "aynı anda tek sahip" garantisi
// ister; bu bir ANLAŞMA problemidir (CRDT'yle çözülemez — kim tutuyor
// sorusunun tek doğru cevabı olmalı). Raft'ın replicated state machine'i
// tam bunu verir: kilit tablosu log'dan deterministik türetilir, tüm
// replikalar aynı sonuca varır.
//
// Üç tasarım kararı:
//
//  1. LEASE (TTL): kilidi tutan çökerse kilit sonsuza kilitlenmemeli.
//     Süre dolunca başkası alabilir. Bedeli: tutan hâlâ çalışıyor ama
//     yavaşladıysa (GC duraklaması, ağ) lease'i dolmuş olabilir —
//     "iki sahip" penceresi.
//
//  2. KARAR STATE MACHINE'DE, ZAMAN LİDERDEN: "alabilir mi?" kararı
//     apply sırasında verilir; wall-clock kullanmak replikaları
//     ayrıştırırdı (her biri farklı sonuç hesaplar). Bu yüzden lider
//     komuta KENDİ zaman damgasını koyar; tüm replikalar o damgayla
//     aynı kararı üretir (determinizm).
//
//  3. FENCING TOKEN: (1)'deki pencerenin doğru çözümü kilidin kendisi
//     değil, korunan kaynağın eski token'ı REDDETMESİdir. Acquire,
//     monoton artan bir token (Raft log index'i) döner; kaynağa her
//     yazmada token taşınır, kaynak gördüğünden küçük token'ı reddeder.
//     (Kleppmann'ın klasik uyarısı; lease tek başına yeterli değildir.)
//
// Her replikanın KENDİ state machine'i vardır (gerçek RSM); okumalar
// aktif liderin state machine'inden yapılır.
package dlock

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"shardlands/pkg/raft"
)

var (
	// ErrNoQuorum: çoğunluk (aktif lider) yok — kilit işlemi yapılamaz.
	// CAP'in C tarafı: bölünmede kilit hizmeti durur.
	ErrNoQuorum = errors.New("dlock: no quorum")
	// ErrTimeout: önerilen komut zamanında commit/apply edilmedi.
	ErrTimeout = errors.New("dlock: timed out waiting for commit")
)

const quorumWindow = 200 * time.Millisecond

// Op kodları (log'a yazılan komutlar).
const (
	opAcquire = "acquire"
	opRenew   = "renew"
	opRelease = "release"
)

type command struct {
	Op    string `json:"op"`
	Key   string `json:"k"`
	Owner string `json:"o"`
	TTLMs int64  `json:"ttl"`
	NowMs int64  `json:"now"` // liderin damgası (determinizm için)
}

// Entry, bir kilidin durumudur.
type Entry struct {
	Owner     string
	ExpiresMs int64
	Token     uint64 // fencing token = acquire'ın Raft log index'i
}

// replica, tek bir Raft düğümü + KENDİ state machine'i.
type replica struct {
	node *raft.Node

	mu      sync.Mutex
	cond    *sync.Cond
	locks   map[string]Entry
	applied uint64
}

func newReplica() *replica {
	r := &replica{locks: map[string]Entry{}}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// apply, replicated state machine: komutu deterministik uygular.
func (r *replica) apply(msg raft.ApplyMsg) {
	var c command
	if err := json.Unmarshal(msg.Cmd, &c); err == nil {
		r.mu.Lock()
		cur, held := r.locks[c.Key]
		expired := held && c.NowMs >= cur.ExpiresMs
		switch c.Op {
		case opAcquire:
			// Boşsa, süresi dolmuşsa veya zaten bizimse: al/yenile.
			if !held || expired || cur.Owner == c.Owner {
				r.locks[c.Key] = Entry{
					Owner:     c.Owner,
					ExpiresMs: c.NowMs + c.TTLMs,
					Token:     msg.Index, // monoton: log index'i
				}
			}
		case opRenew:
			if held && !expired && cur.Owner == c.Owner {
				cur.ExpiresMs = c.NowMs + c.TTLMs
				r.locks[c.Key] = cur // token DEĞİŞMEZ (aynı sahiplik dönemi)
			}
		case opRelease:
			if held && cur.Owner == c.Owner {
				delete(r.locks, c.Key)
			}
		}
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.applied = msg.Index
	r.cond.Broadcast()
	r.mu.Unlock()
}

// waitApplied, bu replikanın index'i uygulamasını bekler.
func (r *replica) waitApplied(index uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.applied < index {
		r.mu.Unlock()
		if time.Now().After(deadline) {
			r.mu.Lock()
			return ErrTimeout
		}
		time.Sleep(2 * time.Millisecond)
		r.mu.Lock()
	}
	return nil
}

func (r *replica) get(key string) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.locks[key]
	return e, ok
}

// Options, kilit kümesinin kurulumu.
type Options struct {
	Replicas                                  int
	ElectionMin, ElectionMax, Heartbeat, Tick time.Duration
	CommitTimeout                             time.Duration
}

func (o *Options) withDefaults() {
	if o.Replicas <= 0 {
		o.Replicas = 3
	}
	if o.ElectionMin <= 0 {
		o.ElectionMin = 150 * time.Millisecond
	}
	if o.ElectionMax <= o.ElectionMin {
		o.ElectionMax = 2 * o.ElectionMin
	}
	if o.Heartbeat <= 0 {
		o.Heartbeat = 50 * time.Millisecond
	}
	if o.Tick <= 0 {
		o.Tick = 10 * time.Millisecond
	}
	if o.CommitTimeout <= 0 {
		o.CommitTimeout = 3 * time.Second
	}
}

// Manager, replikalı kilit hizmetidir.
type Manager struct {
	ids      []string
	reps     map[string]*replica
	nw       *raft.Network
	commitTO time.Duration
}

// New, name önekiyle replicas düğümlü bir kilit kümesi kurar.
func New(name string, opts Options) (*Manager, error) {
	opts.withDefaults()
	m := &Manager{reps: map[string]*replica{}, nw: raft.NewNetwork(), commitTO: opts.CommitTimeout}
	for i := 0; i < opts.Replicas; i++ {
		m.ids = append(m.ids, fmt.Sprintf("%s-l%d", name, i))
	}
	for _, id := range m.ids {
		var peers []string
		for _, p := range m.ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		rep := newReplica()
		n, err := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          m.nw.Transport(id),
			Apply:              rep.apply,
			ElectionTimeoutMin: opts.ElectionMin,
			ElectionTimeoutMax: opts.ElectionMax,
			HeartbeatInterval:  opts.Heartbeat,
			TickInterval:       opts.Tick,
		})
		if err != nil {
			m.Stop()
			return nil, err
		}
		rep.node = n
		m.reps[id] = rep
		m.nw.Register(id, n)
	}
	return m, nil
}

// leader, çoğunlukla teması süren lideri döner.
func (m *Manager) leader() (*replica, bool) {
	for _, id := range m.ids {
		if r := m.reps[id]; r != nil && r.node.QuorumActive(quorumWindow) {
			return r, true
		}
	}
	return nil, false
}

// propose, komutu lidere önerir ve liderin state machine'inde
// uygulanmasını bekler.
func (m *Manager) propose(c command) (*replica, error) {
	lead, ok := m.leader()
	if !ok {
		return nil, ErrNoQuorum
	}
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	idx, _, accepted := lead.node.Propose(data)
	if !accepted {
		return nil, ErrNoQuorum
	}
	if err := lead.waitApplied(idx, m.commitTO); err != nil {
		return nil, err
	}
	return lead, nil
}

// Acquire, key kilidini owner adına ttl süreliğine almayı dener.
// Başarılıysa FENCING TOKEN döner: korunan kaynağa her erişimde bu
// token taşınmalı, kaynak daha küçük token'ı reddetmelidir.
func (m *Manager) Acquire(key, owner string, ttl time.Duration) (token uint64, ok bool, err error) {
	lead, err := m.propose(command{
		Op: opAcquire, Key: key, Owner: owner,
		TTLMs: ttl.Milliseconds(), NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		return 0, false, err
	}
	e, held := lead.get(key)
	if held && e.Owner == owner && time.Now().UnixMilli() < e.ExpiresMs {
		return e.Token, true, nil
	}
	return 0, false, nil
}

// Renew, sahipliği koruyarak lease'i uzatır (token değişmez).
func (m *Manager) Renew(key, owner string, ttl time.Duration) (bool, error) {
	lead, err := m.propose(command{
		Op: opRenew, Key: key, Owner: owner,
		TTLMs: ttl.Milliseconds(), NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		return false, err
	}
	e, held := lead.get(key)
	return held && e.Owner == owner && time.Now().UnixMilli() < e.ExpiresMs, nil
}

// Release, kilidi bırakır (yalnız sahibi bırakabilir).
func (m *Manager) Release(key, owner string) error {
	_, err := m.propose(command{
		Op: opRelease, Key: key, Owner: owner, NowMs: time.Now().UnixMilli(),
	})
	return err
}

// Holder, kilidin güncel (süresi dolmamış) sahibini döner.
func (m *Manager) Holder(key string) (owner string, token uint64, ok bool) {
	lead, has := m.leader()
	if !has {
		return "", 0, false
	}
	e, held := lead.get(key)
	if !held || time.Now().UnixMilli() >= e.ExpiresMs {
		return "", 0, false
	}
	return e.Owner, e.Token, true
}

// ReplicaView, belirli bir replikanın state machine'indeki kaydı döner
// (replikasyonun yakınsadığını doğrulamak için).
func (m *Manager) ReplicaView(replicaID, key string) (Entry, bool) {
	r, ok := m.reps[replicaID]
	if !ok {
		return Entry{}, false
	}
	return r.get(key)
}

// IDs, replika id'leri.
func (m *Manager) IDs() []string { return append([]string(nil), m.ids...) }

// Partition/IsolateAll/Heal: CAP deneyleri.
func (m *Manager) Partition(groups ...[]string) { m.nw.Partition(groups...) }
func (m *Manager) Heal()                        { m.nw.Heal() }
func (m *Manager) IsolateAll() {
	parts := make([][]string, 0, len(m.ids))
	for _, id := range m.ids {
		parts = append(parts, []string{id})
	}
	m.nw.Partition(parts...)
}

// WaitReady, aktif lider oluşana kadar bekler.
func (m *Manager) WaitReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := m.leader(); ok {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (m *Manager) Stop() {
	for _, r := range m.reps {
		if r.node != nil {
			r.node.Stop()
		}
	}
}
