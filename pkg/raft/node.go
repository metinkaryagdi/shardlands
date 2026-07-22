package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Node, tek bir Raft düğümüdür. Tüm durum tek mutex altındadır (etcd
// ve 6.824 stili); RPC gönderirken kilit BIRAKILIR — kilitli RPC,
// iki düğümün birbirini beklemesiyle deadlock üretirdi.
type Node struct {
	mu  sync.Mutex
	cfg Config

	state    State
	term     uint64
	votedFor string
	log      []Entry // log[i] = index i+1 (Raft index'leri 1 tabanlı)

	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64    // lider: her peer'a gönderilecek sıradaki index
	matchIndex  map[string]uint64    // lider: her peer'da bilinen eşleşme
	lastAck     map[string]time.Time // lider: peer'dan en son cevap alınan an

	electionReset   time.Time
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	applyCond *sync.Cond
	stopCh    chan struct{}
	stopped   bool
	rng       *rand.Rand
}

// NewNode, düğümü Storage'daki kalıcı durumdan (varsa) kurar ve
// arka plan döngülerini başlatır.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" || cfg.Transport == nil {
		return nil, errors.New("raft: ID and Transport are required")
	}
	cfg.withDefaults()

	hs, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	seed := time.Now().UnixNano()
	for _, c := range cfg.ID {
		seed = seed*31 + int64(c)
	}
	n := &Node{
		cfg:        cfg,
		state:      Follower,
		term:       hs.Term,
		votedFor:   hs.VotedFor,
		log:        hs.Log,
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		lastAck:    map[string]time.Time{},
		stopCh:     make(chan struct{}),
		rng:        rand.New(rand.NewSource(seed)),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.resetElectionTimerLocked()
	go n.ticker()
	go n.applier()
	return n, nil
}

// Stop, düğümü kalıcı olarak durdurur (crash simülasyonu: Storage
// elde kalır, aynı Storage ile NewNode = restart).
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.applyCond.Broadcast()
}

// QuorumActive, "lider miyim VE son window içinde çoğunlukla temasım
// sürüyor mu" sorusunun cevabıdır (leader lease benzeri).
//
// Neden gerekli? Bölünmüş bir lider, daha yüksek bir dönem görene kadar
// KENDİNİ lider sanmaya devam eder ama commit edemez. Sadece Status()'a
// bakmak "kullanılabilir" yanılgısı verir. QuorumActive bu ikisini
// ayırır: azınlıkta kalan lider false döner — CAP'in C tarafının somut
// ölçümü.
func (n *Node) QuorumActive(window time.Duration) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.state != Leader {
		return false
	}
	count := 1 // kendimiz
	for _, p := range n.cfg.Peers {
		if t, ok := n.lastAck[p]; ok && time.Since(t) <= window {
			count++
		}
	}
	return count*2 > len(n.cfg.Peers)+1
}

// Status: (dönem, lider mi) — testler ve üst katman için.
func (n *Node) Status() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term, n.state == Leader
}

// Propose, komutu log'a önerir. Yalnızca lider kabul eder; kabul
// commit DEĞİLDİR — kayıt çoğunluğa çoğaltılınca commit olur ve Apply
// ile teslim edilir. (Lider devrilirse kabul edilmiş kayıt kaybolabilir;
// commit edilmiş kayıt asla.)
func (n *Node) Propose(cmd []byte) (index, term uint64, isLeader bool) {
	n.mu.Lock()
	if n.stopped || n.state != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	n.log = append(n.log, Entry{Term: n.term, Cmd: append([]byte(nil), cmd...)})
	index, term = n.lastLogIndexLocked(), n.term
	n.persistLocked()
	n.lastHeartbeat = time.Now()
	n.mu.Unlock()
	n.replicateToPeers()
	return index, term, true
}

// ---- zamanlayıcı ----

func (n *Node) ticker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(n.cfg.TickInterval):
		}
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if n.state == Leader {
			if time.Since(n.lastHeartbeat) >= n.cfg.HeartbeatInterval {
				n.lastHeartbeat = time.Now()
				n.mu.Unlock()
				n.replicateToPeers()
				continue
			}
		} else if time.Since(n.electionReset) >= n.electionTimeout {
			n.mu.Unlock()
			n.startElection()
			continue
		}
		n.mu.Unlock()
	}
}

func (n *Node) resetElectionTimerLocked() {
	n.electionReset = time.Now()
	span := int64(n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin)
	n.electionTimeout = n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(span+1))
}

// ---- seçim (§5.2) ----

func (n *Node) startElection() {
	n.mu.Lock()
	if n.stopped || n.state == Leader {
		n.mu.Unlock()
		return
	}
	n.state = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.persistLocked()
	n.resetElectionTimerLocked()
	req := &RequestVoteReq{
		Term:         n.term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastLogIndexLocked(),
		LastLogTerm:  n.termAtLocked(n.lastLogIndexLocked()),
	}
	votes := 1 // kendi oyu
	// Kendi oyu ZATEN çoğunluksa (tek düğümlü küme) hemen lider ol.
	// Çoğunluk kontrolünü yalnızca peer cevabında yapmak, peer'i olmayan
	// bir kümede seçimin hiç sonuçlanmamasına yol açardı.
	if votes*2 > len(n.cfg.Peers)+1 {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		n.replicateToPeers() // peer yoksa no-op
		return
	}
	n.mu.Unlock()

	for _, peer := range n.cfg.Peers {
		go func(peer string) {
			resp, err := n.cfg.Transport.RequestVote(peer, req)
			if err != nil {
				return // ulaşılamayan peer = alınamayan oy; çoğunluk yeter
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.stepDownLocked(resp.Term)
				n.persistLocked()
				return
			}
			// Cevap eski bir seçim turuna aitse veya rol değiştiyse yok say.
			if n.state != Candidate || n.term != req.Term || !resp.Granted {
				return
			}
			votes++
			if votes*2 > len(n.cfg.Peers)+1 {
				n.becomeLeaderLocked()
				go n.replicateToPeers() // otoriteyi hemen ilan et
			}
		}(peer)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	last := n.lastLogIndexLocked()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1 // iyimser başla, çakışmada geri yürü
		n.matchIndex[p] = 0
	}
	n.lastHeartbeat = time.Now()
}

// stepDownLocked: daha yüksek dönem görüldü — kim olursak olalım
// follower'a düş. Raft'ın tek yönlü saati: dönem asla geri gitmez.
func (n *Node) stepDownLocked(term uint64) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.state = Follower
	n.resetElectionTimerLocked()
}

// ---- replikasyon (§5.3) ----

func (n *Node) replicateToPeers() {
	for _, p := range n.cfg.Peers {
		go n.replicateTo(p)
	}
}

func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.stopped || n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.term
	next := n.nextIndex[peer]
	prevIdx := next - 1
	req := &AppendEntriesReq{
		Term:         term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  n.termAtLocked(prevIdx),
		Entries:      append([]Entry(nil), n.log[next-1:]...),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.cfg.Transport.AppendEntries(peer, req)
	if err != nil {
		return // heartbeat döngüsü zaten tekrar deneyecek
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.stepDownLocked(resp.Term)
		n.persistLocked()
		return
	}
	if n.state != Leader || n.term != term {
		return // bu cevap eski bir liderlik dönemine ait
	}
	// Peer'dan cevap geldi: temas kaydı (QuorumActive/lease için).
	n.lastAck[peer] = time.Now()
	if resp.Success {
		m := prevIdx + uint64(len(req.Entries))
		if m > n.matchIndex[peer] {
			n.matchIndex[peer] = m
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitLocked()
		return
	}
	// Tutarlılık reddi: peer'ın log'u bizimkiyle prevIdx'te uyuşmuyor.
	// Bir geri adım at ve hemen tekrar dene (en fazla log boyu kadar).
	if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	go n.replicateTo(peer)
}

// advanceCommitLocked (§5.3, §5.4.2): N, çoğunlukta VE KENDİ
// dönemimizden ise commit. Eski dönem kayıtları asla sayımla commit
// edilmez — Figure 8 senaryosunda üzerine yazılabilirler; kendi
// dönemimizden bir kayıt commit olunca önceki her şey dolaylı commit olur.
func (n *Node) advanceCommitLocked() {
	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {
		if n.termAtLocked(N) != n.term {
			break // daha eski dönemler: doğrudan commit yasak
		}
		count := 1 // kendimiz
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count*2 > len(n.cfg.Peers)+1 {
			n.commitIndex = N
			n.applyCond.Broadcast()
			break
		}
	}
}

// ---- RPC handler'ları ----

func (n *Node) HandleRequestVote(req *RequestVoteReq) (*RequestVoteResp, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrUnreachable
	}
	changed := false
	if req.Term > n.term {
		n.stepDownLocked(req.Term)
		changed = true
	}
	resp := &RequestVoteResp{Term: n.term}
	if req.Term < n.term {
		if changed {
			n.persistLocked()
		}
		return resp, nil
	}
	// Oy güvenliği (§5.4.1): adayın log'u en az bizimki kadar
	// güncel olmalı — önce son dönem, eşitse uzunluk. Bu kural,
	// commit edilmiş kaydı taşımayan birinin lider olmasını engeller.
	lastIdx := n.lastLogIndexLocked()
	lastTerm := n.termAtLocked(lastIdx)
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		resp.Granted = true
		n.resetElectionTimerLocked() // oy verdik; adaya süre tanı
		changed = true
	}
	if changed {
		n.persistLocked() // persist-then-respond: crash sonrası çifte oy yasak
	}
	return resp, nil
}

func (n *Node) HandleAppendEntries(req *AppendEntriesReq) (*AppendEntriesResp, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrUnreachable
	}
	changed := false
	if req.Term > n.term {
		n.stepDownLocked(req.Term)
		changed = true
	}
	resp := &AppendEntriesResp{Term: n.term}
	if req.Term < n.term {
		if changed {
			n.persistLocked()
		}
		return resp, nil
	}
	// Bu dönemin meşru liderinden ses geldi: candidate isek çekil,
	// seçim saatini sıfırla.
	n.state = Follower
	n.resetElectionTimerLocked()

	// Tutarlılık kontrolü: prev noktasında log'larımız aynı mı?
	if req.PrevLogIndex > n.lastLogIndexLocked() ||
		n.termAtLocked(req.PrevLogIndex) != req.PrevLogTerm {
		if changed {
			n.persistLocked()
		}
		return resp, nil // Success=false → lider nextIndex'i geri yürütür
	}
	// Eşleşen noktadan itibaren: aynı (index,term) taşıyan kayıtlara
	// dokunma (idempotentlik — RPC'ler tekrar gelebilir); ilk çelişkide
	// kuyruğu kes ve liderinkini yaz. Commit edilmiş kayıt burada asla
	// kesilmez: commit çoğunlukta demektir, lider onu taşımak zorundadır.
	for i, e := range req.Entries {
		at := req.PrevLogIndex + uint64(i) + 1
		if at > n.lastLogIndexLocked() {
			n.log = append(n.log, req.Entries[i:]...)
			changed = true
			break
		}
		if n.termAtLocked(at) != e.Term {
			n.log = append(n.log[:at-1], req.Entries[i:]...)
			changed = true
			break
		}
	}
	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, lastNew)
		n.applyCond.Broadcast()
	}
	if changed {
		n.persistLocked()
	}
	resp.Success = true
	return resp, nil
}

// ---- apply döngüsü ----

func (n *Node) applier() {
	n.mu.Lock()
	for {
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		start, end := n.lastApplied+1, n.commitIndex
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, ApplyMsg{Index: i, Term: n.log[i-1].Term, Cmd: n.log[i-1].Cmd})
		}
		n.lastApplied = end
		n.mu.Unlock()
		for _, m := range msgs {
			n.cfg.Apply(m) // kilitsiz: Apply bloklarsa yalnız applier durur
		}
		n.mu.Lock()
	}
}

// ---- yardımcılar ----

func (n *Node) lastLogIndexLocked() uint64 { return uint64(len(n.log)) }

func (n *Node) termAtLocked(idx uint64) uint64 {
	if idx == 0 || idx > uint64(len(n.log)) {
		return 0
	}
	return n.log[idx-1].Term
}

func (n *Node) persistLocked() {
	if err := n.cfg.Storage.Save(HardState{Term: n.term, VotedFor: n.votedFor, Log: n.log}); err != nil {
		// Persist edemeyen düğümün oy/log sözü geçersizdir; güvenli
		// davranış çökmek olurdu. Test storage'ı hata üretmez; gerçek
		// entegrasyonda (Faz 3) bu yol panic/fatal olacak.
		panic("raft: persist failed: " + err.Error())
	}
}
