package dlock

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func fastOpts() Options {
	return Options{
		Replicas:      3,
		ElectionMin:   60 * time.Millisecond,
		ElectionMax:   120 * time.Millisecond,
		Heartbeat:     20 * time.Millisecond,
		Tick:          5 * time.Millisecond,
		CommitTimeout: time.Second,
	}
}

// waitNoLeader, aktif lider kalmayana kadar bekler (izolasyon sonrası
// lease penceresi kapanmalı).
func waitNoLeader(t *testing.T, m *Manager, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := m.leader(); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("an active leader remained after isolation")
}

func newManager(t *testing.T) *Manager {
	t.Helper()
	m, err := New("t"+t.Name(), fastOpts())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	if !m.WaitReady(5 * time.Second) {
		t.Fatal("lock cluster never elected a leader")
	}
	return m
}

// Temel: al, sahibi gör, bırak.
func TestAcquireHolderRelease(t *testing.T) {
	m := newManager(t)

	tok, ok, err := m.Acquire("region/r-0-0", "nodeA", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire = %v,%v", ok, err)
	}
	if tok == 0 {
		t.Fatal("fencing token must be non-zero")
	}
	owner, tok2, held := m.Holder("region/r-0-0")
	if !held || owner != "nodeA" || tok2 != tok {
		t.Fatalf("holder = %q,%d,%v want nodeA,%d,true", owner, tok2, held, tok)
	}

	if err := m.Release("region/r-0-0", "nodeA"); err != nil {
		t.Fatal(err)
	}
	if _, _, held := m.Holder("region/r-0-0"); held {
		t.Fatal("lock still held after release")
	}
}

// Karşılıklı dışlama: A tutarken B alamaz; A bırakınca B alır.
func TestMutualExclusion(t *testing.T) {
	m := newManager(t)
	key := "handoff/p-1"

	if _, ok, _ := m.Acquire(key, "A", time.Minute); !ok {
		t.Fatal("A could not acquire")
	}
	if _, ok, err := m.Acquire(key, "B", time.Minute); err != nil || ok {
		t.Fatalf("B acquired while A holds (ok=%v err=%v)", ok, err)
	}
	if owner, _, _ := m.Holder(key); owner != "A" {
		t.Fatalf("holder = %q, want A", owner)
	}

	m.Release(key, "A")
	if _, ok, _ := m.Acquire(key, "B", time.Minute); !ok {
		t.Fatal("B could not acquire after A released")
	}
}

// Yalnız sahibi bırakabilir: yabancı Release kilidi düşürmemeli.
func TestReleaseOnlyByOwner(t *testing.T) {
	m := newManager(t)
	key := "k"
	m.Acquire(key, "A", time.Minute)
	if err := m.Release(key, "B"); err != nil {
		t.Fatal(err)
	}
	if owner, _, held := m.Holder(key); !held || owner != "A" {
		t.Fatalf("foreign release dropped the lock (owner=%q held=%v)", owner, held)
	}
}

// Lease süresi dolunca başkası alabilir; token ARTAR (fencing).
func TestLeaseExpiryAndFencingTokenGrows(t *testing.T) {
	m := newManager(t)
	key := "lease"

	tok1, ok, _ := m.Acquire(key, "A", 120*time.Millisecond)
	if !ok {
		t.Fatal("A could not acquire")
	}
	// Süre dolmadan B alamaz.
	if _, ok, _ := m.Acquire(key, "B", time.Minute); ok {
		t.Fatal("B acquired before A's lease expired")
	}
	time.Sleep(200 * time.Millisecond) // lease dolsun

	tok2, ok, err := m.Acquire(key, "B", time.Minute)
	if err != nil || !ok {
		t.Fatalf("B could not acquire after expiry: %v %v", ok, err)
	}
	if tok2 <= tok1 {
		t.Fatalf("fencing token did not grow: %d -> %d (stale writer could not be fenced)", tok1, tok2)
	}
}

// Renew: lease uzar, sahiplik korunur, token DEĞİŞMEZ.
func TestRenewKeepsOwnershipAndToken(t *testing.T) {
	m := newManager(t)
	key := "renew"

	tok, ok, _ := m.Acquire(key, "A", 150*time.Millisecond)
	if !ok {
		t.Fatal("acquire failed")
	}
	time.Sleep(80 * time.Millisecond)
	renewed, err := m.Renew(key, "A", 500*time.Millisecond)
	if err != nil || !renewed {
		t.Fatalf("renew = %v,%v", renewed, err)
	}
	time.Sleep(120 * time.Millisecond) // ilk lease dolardı, yenilendi

	owner, tok2, held := m.Holder(key)
	if !held || owner != "A" {
		t.Fatalf("renewed lock lost: owner=%q held=%v", owner, held)
	}
	if tok2 != tok {
		t.Fatalf("token changed on renew: %d -> %d (same ownership epoch)", tok, tok2)
	}
	if _, ok, _ := m.Acquire(key, "B", time.Minute); ok {
		t.Fatal("B acquired while A's renewed lease is alive")
	}
}

// Eşzamanlı yarış: N istemci aynı kilidi ister, TAM BİRİ kazanır.
func TestConcurrentAcquireSingleWinner(t *testing.T) {
	m := newManager(t)
	key := "race"

	const n = 8
	var wg sync.WaitGroup
	winners := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, ok, err := m.Acquire(key, fmt.Sprintf("c%d", i), time.Minute); ok && err == nil {
				winners <- fmt.Sprintf("c%d", i)
			}
		}(i)
	}
	wg.Wait()
	close(winners)

	count := 0
	for range winners {
		count++
	}
	if count != 1 {
		t.Fatalf("%d clients acquired the same lock, want exactly 1", count)
	}
}

// Replikasyon: tüm replikaların state machine'leri aynı sahipte
// yakınsamalı (replicated state machine).
func TestReplicasConverge(t *testing.T) {
	m := newManager(t)
	key := "repl"
	tok, ok, _ := m.Acquire(key, "A", time.Minute)
	if !ok {
		t.Fatal("acquire failed")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		agree := 0
		for _, id := range m.IDs() {
			if e, held := m.ReplicaView(id, key); held && e.Owner == "A" && e.Token == tok {
				agree++
			}
		}
		if agree == len(m.IDs()) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("replicas did not converge on lock state")
}

// Çoğunluk yoksa kilit hizmeti durur (CAP: C). Kilit ANLAŞMA
// gerektirir; bölünmede kullanılabilirlikten vazgeçilir.
func TestNoQuorumBlocksLocking(t *testing.T) {
	m := newManager(t)
	if _, ok, _ := m.Acquire("k", "A", time.Minute); !ok {
		t.Fatal("baseline acquire failed")
	}

	m.IsolateAll()
	waitNoLeader(t, m, 3*time.Second)

	// Lider yokken kilit alınamaz.
	if _, ok, err := m.Acquire("k2", "B", time.Minute); ok || !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("acquire without quorum = ok:%v err:%v, want ErrNoQuorum", ok, err)
	}
	if _, _, held := m.Holder("k"); held {
		t.Fatal("Holder must not answer without an active leader")
	}

	// İyileşince kilit durumu KORUNMUŞ olmalı (log replike edilmişti).
	m.Heal()
	if !m.WaitReady(5 * time.Second) {
		t.Fatal("cluster did not recover")
	}
	if owner, _, held := m.Holder("k"); !held || owner != "A" {
		t.Fatalf("lock state lost after partition: owner=%q held=%v", owner, held)
	}
}

// Lider failover'ında kilit durumu korunur (replike log).
func TestLockSurvivesLeaderFailover(t *testing.T) {
	m := newManager(t)
	key := "failover"
	tok, ok, _ := m.Acquire(key, "A", time.Minute)
	if !ok {
		t.Fatal("acquire failed")
	}

	// Aktif lideri bul ve izole et.
	var oldLeader string
	for _, id := range m.IDs() {
		if r := m.reps[id]; r.node.QuorumActive(quorumWindow) {
			oldLeader = id
			break
		}
	}
	if oldLeader == "" {
		t.Fatal("no leader to isolate")
	}
	m.Partition([]string{oldLeader})

	// Kalan çoğunluk yeni lider seçmeli ve kilidi bilmeli.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if owner, tok2, held := m.Holder(key); held && owner == "A" && tok2 == tok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("lock state not preserved across leader failover")
}
