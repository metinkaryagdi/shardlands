package handoff

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"shardlands/pkg/dlock"
	"shardlands/pkg/es"
	"shardlands/services/arena"
)

// fakePort, oturum yerine geçer: aldığı token'ları kaydeder, istenirse
// EnterArena sırasında bloklar (kilit tutulurken yarış kurmak için).
type fakePort struct {
	mu         sync.Mutex
	tokens     []uint64
	inArena    bool
	block      chan struct{}
	blocked    chan struct{}
	blockedOne sync.Once
	failWith   error
}

func (p *fakePort) EnterArena(_ string, _ *arena.Arena, _ int, token uint64) error {
	p.mu.Lock()
	fail, block, blocked := p.failWith, p.block, p.blocked
	p.mu.Unlock()

	if block != nil {
		if blocked != nil {
			p.blockedOne.Do(func() { close(blocked) })
		}
		<-block
	}
	if fail != nil {
		return fail
	}
	p.mu.Lock()
	p.tokens = append(p.tokens, token)
	p.inArena = true
	p.mu.Unlock()
	return nil
}

func (p *fakePort) EnterHub(_ string, token uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tokens = append(p.tokens, token)
	p.inArena = false
	return nil
}

func (p *fakePort) seen() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint64(nil), p.tokens...)
}

func testStore(t *testing.T) *es.Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-handoff-*")
	if err != nil {
		t.Fatal(err)
	}
	s, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); os.RemoveAll(dir) })
	return s
}

func testLocks(t *testing.T) *dlock.Manager {
	t.Helper()
	m, err := dlock.New("t"+t.Name(), dlock.Options{
		Replicas:      3,
		ElectionMin:   60 * time.Millisecond,
		ElectionMax:   120 * time.Millisecond,
		Heartbeat:     20 * time.Millisecond,
		Tick:          5 * time.Millisecond,
		CommitTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	if !m.WaitReady(5 * time.Second) {
		t.Fatal("lock cluster not ready")
	}
	return m
}

func testArena() *arena.Arena {
	return arena.New("arena-x", arena.Mode1v1, []arena.PlayerSpec{
		{ID: "p1", Team: 0}, {ID: "p2", Team: 1},
	}, arena.Options{})
}

func auditTypes(t *testing.T, store *es.Store, playerID string) []string {
	t.Helper()
	evs, err := store.ReadStream(Stream(playerID), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

// Mutlu yol: arenaya geçiş ve hub'a dönüş denetim izine yazılır.
func TestToArenaAndBackAudits(t *testing.T) {
	store := testStore(t)
	port := &fakePort{}
	c := New(testLocks(t), store, port, "gw-1")

	if err := c.ToArena("p1", testArena(), 0); err != nil {
		t.Fatalf("ToArena: %v", err)
	}
	if got := auditTypes(t, store, "p1"); len(got) != 2 ||
		got[0] != EventLeftHub || got[1] != EventEnteredArena {
		t.Fatalf("audit = %v, want [LeftHub EnteredArena]", got)
	}

	if err := c.ToHub("p1"); err != nil {
		t.Fatalf("ToHub: %v", err)
	}
	got := auditTypes(t, store, "p1")
	if len(got) != 4 || got[2] != EventLeftArena || got[3] != EventEnteredHub {
		t.Fatalf("audit = %v, want ...[LeftArena EnteredHub]", got)
	}
}

// Fencing: her transfer ARTAN bir token alır — oturum bayat emri
// bu sayede reddedebilir.
func TestTokensIncreaseAcrossTransfers(t *testing.T) {
	port := &fakePort{}
	c := New(testLocks(t), nil, port, "gw-1")

	if err := c.ToArena("p1", testArena(), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.ToHub("p1"); err != nil {
		t.Fatal(err)
	}
	if err := c.ToArena("p1", testArena(), 1); err != nil {
		t.Fatal(err)
	}
	tokens := port.seen()
	if len(tokens) != 3 {
		t.Fatalf("tokens = %v, want 3", tokens)
	}
	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Fatalf("tokens not increasing: %v (fencing broken)", tokens)
		}
	}
}

// ÇİFTE TRANSFER ENGELİ: bir transfer sürerken aynı oyuncu için ikinci
// transfer kilide takılır ve ErrBusy alır.
func TestConcurrentTransferBlocked(t *testing.T) {
	port := &fakePort{
		block:   make(chan struct{}),
		blocked: make(chan struct{}),
	}
	c := New(testLocks(t), nil, port, "gw-1")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.ToArena("p1", testArena(), 0); err != nil {
			t.Errorf("first transfer failed: %v", err)
		}
	}()

	<-port.blocked // ilk transfer kilidi tutuyor ve port'ta bekliyor

	// İkinci transfer aynı oyuncu için: kilit alınamamalı.
	err := c.ToArena("p1", testArena(), 1)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("second concurrent transfer = %v, want ErrBusy", err)
	}

	close(port.block)
	wg.Wait()

	// Kilit bırakıldıktan sonra yeni transfer geçmeli.
	port.mu.Lock()
	port.block, port.blocked = nil, nil
	port.mu.Unlock()
	if err := c.ToHub("p1"); err != nil {
		t.Fatalf("transfer after release: %v", err)
	}
}

// Farklı oyuncular birbirini engellemez (kilit oyuncu başına).
func TestDifferentPlayersDoNotBlock(t *testing.T) {
	port := &fakePort{}
	c := New(testLocks(t), nil, port, "gw-1")

	if err := c.ToArena("p1", testArena(), 0); err != nil {
		t.Fatalf("p1: %v", err)
	}
	if err := c.ToArena("p2", testArena(), 1); err != nil {
		t.Fatalf("p2 blocked by p1's lock: %v", err)
	}
}

// Oturum emri reddederse transfer başarısız sayılır ve HandoffFailed
// yazılır (yanlışlıkla "taşındı" denmez).
func TestPortFailureRecorded(t *testing.T) {
	store := testStore(t)
	port := &fakePort{failWith: errors.New("oturum kapalı")}
	c := New(testLocks(t), store, port, "gw-1")

	if err := c.ToArena("p1", testArena(), 0); err == nil {
		t.Fatal("expected error when session rejects")
	}
	got := auditTypes(t, store, "p1")
	if len(got) != 1 || got[0] != EventHandoffFailed {
		t.Fatalf("audit = %v, want [HandoffFailed]", got)
	}
}
