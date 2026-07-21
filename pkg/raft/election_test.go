package raft

import (
	"testing"
	"time"
)

// 3 düğüm: tek lider seçilmeli.
func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(3 * time.Second)
}

// Lider çökünce kalan çoğunluk (2/3) yeni lider seçmeli.
func TestLeaderCrashFailover(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader(3 * time.Second)
	c.crash(old)

	var rest []string
	for _, id := range c.ids {
		if id != old {
			rest = append(rest, id)
		}
	}
	newLeader := c.waitLeaderAmong(rest, 3*time.Second)
	if newLeader == old {
		t.Fatalf("crashed node %s cannot stay leader", old)
	}
}

// Çoğunluk yoksa lider seçilemez: 3 düğümden 2'si çökünce kalan tek
// düğüm asla lider olamamalı (2 oy gerekir, 1'i var).
func TestNoQuorumNoLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(3 * time.Second)

	var survivor string
	for _, id := range c.ids {
		if id != leader {
			if survivor == "" {
				survivor = id
				continue
			}
			c.crash(id)
		}
	}
	c.crash(leader)

	// Birkaç seçim zaman aşımı boyunca gözle: aday olur, oy toplayamaz,
	// dönem büyütür ama LİDER olamaz.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, isLeader := c.node(survivor).Status(); isLeader {
			t.Fatal("node without quorum must never become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Partition testi: lider azınlıkta kalınca çoğunluk yeni lider seçer;
// bölünme iyileşince eski lider (daha yüksek dönemi görüp) çekilir ve
// kümede tek lider kalır.
func TestPartitionedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader(3 * time.Second)

	c.nw.Isolate(old) // eski lider tek başına azınlıkta

	var majority []string
	for _, id := range c.ids {
		if id != old {
			majority = append(majority, id)
		}
	}
	c.waitLeaderAmong(majority, 3*time.Second)

	c.nw.Heal()

	// İyileşme sonrası: küme genelinde tek lider oturmalı ve bu lider
	// eski lider OLMAMALI (çoğunluk tarafının dönemi daha yüksek).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []string
		for _, id := range c.ids {
			if _, isLeader := c.node(id).Status(); isLeader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 && leaders[0] != old {
			oldTerm, _ := c.node(old).Status()
			newTerm, _ := c.node(leaders[0]).Status()
			if oldTerm != newTerm {
				t.Fatalf("terms did not converge: old=%d new=%d", oldTerm, newTerm)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cluster did not settle on a single (new) leader after heal")
}

// Oy güvenliği (§5.4.1) birim testi: log'u geride kalan aday,
// handler seviyesinde reddedilmeli.
func TestVoteRejectsStaleLog(t *testing.T) {
	nw := NewNetwork()
	n, err := NewNode(Config{
		ID: "solo", Peers: nil, Transport: nw.Transport("solo"),
		// Uzun zaman aşımı: test sırasında kendi seçimini başlatmasın.
		ElectionTimeoutMin: time.Hour, ElectionTimeoutMax: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Düğüme elle log ver: dönem 3'ten 2 kayıt.
	n.mu.Lock()
	n.term = 3
	n.log = []Entry{{Term: 2}, {Term: 3}}
	n.mu.Unlock()

	// Aday dönem olarak önde ama log'u eski dönemde bitiyor: RED.
	resp, err := n.HandleRequestVote(&RequestVoteReq{
		Term: 5, CandidateID: "behind", LastLogIndex: 5, LastLogTerm: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Granted {
		t.Fatal("stale-log candidate must be rejected despite higher term")
	}
	if resp.Term != 5 {
		t.Fatalf("term must advance to 5, got %d", resp.Term)
	}

	// Log'u güncel aday (son dönem eşit, index uzun): KABUL.
	resp, err = n.HandleRequestVote(&RequestVoteReq{
		Term: 5, CandidateID: "fresh", LastLogIndex: 3, LastLogTerm: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Granted {
		t.Fatal("up-to-date candidate must be granted")
	}

	// Aynı dönemde ikinci oy: RED (votedFor dolu).
	resp, _ = n.HandleRequestVote(&RequestVoteReq{
		Term: 5, CandidateID: "second", LastLogIndex: 9, LastLogTerm: 5,
	})
	if resp.Granted {
		t.Fatal("second vote in same term must be rejected")
	}
}
