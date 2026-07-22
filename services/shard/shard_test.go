package shard

import (
	"os"
	"testing"
	"time"
)

// fastOpts: testler için sıkıştırılmış ama oranı korunmuş zamanlamalar.
func fastOpts(replicas int) Options {
	return Options{
		Replicas:    replicas,
		ElectionMin: 60 * time.Millisecond,
		ElectionMax: 120 * time.Millisecond,
		Heartbeat:   20 * time.Millisecond,
		Tick:        5 * time.Millisecond,
	}
}

func waitAvailable(t *testing.T, g *Group, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if g.Available() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("group %s available=%v, want %v", g.ID, g.Available(), want)
}

// 3 replikalı grup lider seçer ve kullanılabilir olur.
func TestGroupElectsLeader(t *testing.T) {
	g, err := NewGroup("shard-x", fastOpts(3))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Stop()

	waitAvailable(t, g, true, 3*time.Second)
	id, ok := g.Leader()
	if !ok || id == "" {
		t.Fatalf("Leader() = %q,%v", id, ok)
	}
	if !g.Propose([]byte("hello")) {
		t.Fatal("propose to healthy group must succeed")
	}
}

// Lider izole olunca kalan çoğunluk yeni lider seçer: shard yine
// kullanılabilir (failover).
func TestLeaderIsolationFailsOver(t *testing.T) {
	g, err := NewGroup("shard-y", fastOpts(3))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Stop()
	waitAvailable(t, g, true, 3*time.Second)

	old, _ := g.Leader()
	g.Partition([]string{old}) // eski lideri tek başına bırak

	// Kalan iki düğüm çoğunluk: yeni lider seçilmeli.
	deadline := time.Now().Add(3 * time.Second)
	var newLeader string
	for time.Now().Before(deadline) {
		if id, ok := g.Leader(); ok && id != old {
			newLeader = id
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatal("no failover leader after isolating the old one")
	}
	if !g.Propose([]byte("after-failover")) {
		t.Fatal("propose must succeed on the new majority leader")
	}
}

// Çoğunluk hiçbir tarafta yoksa lider YOK: shard kullanılamaz (CAP: C).
// Not: eski lider kendini lider sanmaya devam edebilir; QuorumActive
// bunu ayırt eder.
func TestNoQuorumMakesShardUnavailable(t *testing.T) {
	g, err := NewGroup("shard-z", fastOpts(3))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Stop()
	waitAvailable(t, g, true, 3*time.Second)

	g.IsolateAll()
	waitAvailable(t, g, false, 3*time.Second)

	if g.Propose([]byte("nope")) {
		t.Fatal("propose must fail without quorum")
	}

	// İyileşince yeniden kullanılabilir olmalı.
	g.Heal()
	waitAvailable(t, g, true, 3*time.Second)
}

// Manager: birden çok shard; birini indirmek diğerini etkilemez
// (gruplar yalıtık ağlarda).
func TestManagerShardIsolationIndependent(t *testing.T) {
	m, err := NewManager([]string{"shard-0", "shard-1"}, fastOpts(3))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	if !m.WaitReady(5 * time.Second) {
		t.Fatal("shards did not become ready")
	}

	m.Group("shard-0").IsolateAll()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && m.Available("shard-0") {
		time.Sleep(10 * time.Millisecond)
	}
	if m.Available("shard-0") {
		t.Fatal("shard-0 must be unavailable after isolation")
	}
	if !m.Available("shard-1") {
		t.Fatal("shard-1 must stay available (isolated networks)")
	}
	if m.Available("bilinmeyen") {
		t.Fatal("unknown shard must not be available")
	}
}

// Kalıcı depo: gruplar LSM tabanlı raftstore ile kurulabilir ve
// çalışır (Faz 0 Raft + Faz 0 LSM birleşimi).
func TestGroupWithPersistentStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "shardlands-shard-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	opts := fastOpts(3)
	opts.DataDir = dir
	g, err := NewGroup("shard-p", opts)
	if err != nil {
		t.Fatal(err)
	}
	waitAvailable(t, g, true, 5*time.Second)
	if !g.Propose([]byte("durable")) {
		t.Fatal("propose failed")
	}
	// Kısa bir replikasyon payı, sonra kapat.
	time.Sleep(200 * time.Millisecond)
	g.Stop()

	// Aynı dizinle yeniden kurulunca açılabilmeli (durum diskten okunur).
	g2, err := NewGroup("shard-p", opts)
	if err != nil {
		t.Fatalf("reopen with persisted state: %v", err)
	}
	defer g2.Stop()
	waitAvailable(t, g2, true, 5*time.Second)
}
