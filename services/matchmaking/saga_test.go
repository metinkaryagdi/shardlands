package matchmaking

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"shardlands/pkg/es"
	"shardlands/services/arena"
)

func testStore(t *testing.T) *es.Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-mm-*")
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

// recordingAssigner, atamaları kaydeder ve istenirse belirli bir
// oyuncuda hata verir (telafi testleri).
type recordingAssigner struct {
	mu       sync.Mutex
	assigned []string
	released []string
	failOn   string
}

func (a *recordingAssigner) Assign(playerID string, h *Handle, team int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if playerID == a.failOn {
		return errors.New("atama reddedildi")
	}
	a.assigned = append(a.assigned, playerID)
	return nil
}

func (a *recordingAssigner) Release(playerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.released = append(a.released, playerID)
}

func (a *recordingAssigner) snapshot() (assigned, released []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.assigned...), append([]string(nil), a.released...)
}

// failingProvisioner, provision adımında hata verir.
type failingProvisioner struct{ calls int }

func (p *failingProvisioner) Provision(context.Context, ArenaSpec) (*Handle, error) {
	p.calls++
	return nil, errors.New("kapasite yok")
}
func (p *failingProvisioner) Destroy(context.Context, string) error { return nil }

func enqueueAll(t *testing.T, m *Matcher, mode string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		m.Register(QueuedPlayer{ID: id, Name: id})
		if _, err := m.Enqueue(id, mode); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
}

func waitMatches(t *testing.T, m *Matcher, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.Matches() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("matches = %d, want %d", m.Matches(), want)
}

func readMatchEvents(t *testing.T, store *es.Store, matchID string) []string {
	t.Helper()
	evs, err := store.ReadStream(MatchStream(matchID), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	types := make([]string, len(evs))
	for i, e := range evs {
		types[i] = e.Type
	}
	return types
}

// Mutlu yol: iki oyuncu 1v1 kuyruğuna girer, arena sağlanır, ikisi de
// atanır ve denetim izi tam olur.
func TestSagaHappyPath1v1(t *testing.T) {
	store := testStore(t)
	prov := NewLocalProvisioner()
	asg := &recordingAssigner{}
	m := NewMatcher(store, prov, asg)

	enqueueAll(t, m, "1v1", "p1", "p2")
	waitMatches(t, m, 1)

	if prov.Count() != 1 {
		t.Fatalf("arenas = %d, want 1", prov.Count())
	}
	assigned, _ := asg.snapshot()
	if len(assigned) != 2 {
		t.Fatalf("assigned = %v, want 2 players", assigned)
	}
	if m.QueueLen("1v1") != 0 {
		t.Fatalf("queue not drained: %d", m.QueueLen("1v1"))
	}
	if got := readMatchEvents(t, store, "m1"); len(got) != 3 ||
		got[0] != EventMatchFormed || got[1] != EventArenaProvisioned || got[2] != EventPlayersAssigned {
		t.Fatalf("audit trail = %v", got)
	}
}

// 2v2 dört oyuncu ister; üç oyuncuyla maç kurulmaz.
func TestSagaWaitsForFullTeams(t *testing.T) {
	m := NewMatcher(nil, NewLocalProvisioner(), &recordingAssigner{})
	enqueueAll(t, m, "2v2", "a", "b", "c")
	time.Sleep(200 * time.Millisecond)
	if m.Matches() != 0 {
		t.Fatal("match formed with 3 players in 2v2")
	}
	if m.QueueLen("2v2") != 3 {
		t.Fatalf("queue = %d, want 3", m.QueueLen("2v2"))
	}

	enqueueAll(t, m, "2v2", "d")
	waitMatches(t, m, 1)
	if m.QueueLen("2v2") != 0 {
		t.Fatalf("queue after match = %d, want 0", m.QueueLen("2v2"))
	}
}

// TELAFİ: provision başarısızsa oyuncular kuyruğa iade edilir ve iptal
// kaydedilir; sızan arena olmaz.
func TestSagaProvisionFailureRequeues(t *testing.T) {
	store := testStore(t)
	prov := &failingProvisioner{}
	asg := &recordingAssigner{}
	m := NewMatcher(store, prov, asg)

	enqueueAll(t, m, "1v1", "p1", "p2")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && m.QueueLen("1v1") < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if m.QueueLen("1v1") != 2 {
		t.Fatalf("players not requeued: queue = %d", m.QueueLen("1v1"))
	}
	if m.Matches() != 0 {
		t.Fatal("match counted despite provision failure")
	}
	if assigned, _ := asg.snapshot(); len(assigned) != 0 {
		t.Fatalf("players assigned despite provision failure: %v", assigned)
	}
	got := readMatchEvents(t, store, "m1")
	if len(got) != 2 || got[0] != EventMatchFormed || got[1] != EventMatchCancelled {
		t.Fatalf("audit trail = %v, want [MatchFormed MatchCancelled]", got)
	}
}

// TELAFİ: atama ortada başarısızsa arena YIKILIR, atananlar geri alınır,
// herkes kuyruğa iade edilir.
func TestSagaAssignFailureCompensates(t *testing.T) {
	store := testStore(t)
	prov := NewLocalProvisioner()
	asg := &recordingAssigner{failOn: "p2"} // ikinci oyuncuda patla
	m := NewMatcher(store, prov, asg)

	enqueueAll(t, m, "1v1", "p1", "p2")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && m.QueueLen("1v1") < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if m.QueueLen("1v1") != 2 {
		t.Fatalf("players not requeued: %d", m.QueueLen("1v1"))
	}
	if prov.Count() != 0 {
		t.Fatalf("arena leaked after assign failure: %d", prov.Count())
	}
	_, released := asg.snapshot()
	if len(released) != 1 || released[0] != "p1" {
		t.Fatalf("released = %v, want [p1] (already-assigned player rolled back)", released)
	}
	got := readMatchEvents(t, store, "m1")
	if len(got) != 3 || got[2] != EventMatchCancelled {
		t.Fatalf("audit trail = %v, want ...MatchCancelled", got)
	}
}

// Kuyruk idempotentliği: aynı oyuncu iki kez sıraya girerse sırası
// değişmez ve maç iki oyuncu birikmeden kurulmaz.
func TestEnqueueIdempotent(t *testing.T) {
	m := NewMatcher(nil, NewLocalProvisioner(), &recordingAssigner{})
	m.Register(QueuedPlayer{ID: "p1", Name: "p1"})

	pos1, _ := m.Enqueue("p1", "1v1")
	pos2, _ := m.Enqueue("p1", "1v1")
	if pos1 != 1 || pos2 != 1 {
		t.Fatalf("positions = %d,%d, want 1,1", pos1, pos2)
	}
	if m.QueueLen("1v1") != 1 {
		t.Fatalf("queue = %d, want 1 (no duplicate)", m.QueueLen("1v1"))
	}
	time.Sleep(100 * time.Millisecond)
	if m.Matches() != 0 {
		t.Fatal("match formed from a single duplicated player")
	}
}

// Bağlantısı kopan oyuncu kuyruktan düşer.
func TestUnregisterRemovesFromQueue(t *testing.T) {
	m := NewMatcher(nil, NewLocalProvisioner(), &recordingAssigner{})
	enqueueAll(t, m, "1v1", "p1")
	if m.QueueLen("1v1") != 1 {
		t.Fatal("not queued")
	}
	m.Unregister("p1")
	if m.QueueLen("1v1") != 0 {
		t.Fatalf("queue = %d after unregister, want 0", m.QueueLen("1v1"))
	}
}

func TestEnqueueUnknownMode(t *testing.T) {
	m := NewMatcher(nil, NewLocalProvisioner(), &recordingAssigner{})
	if _, err := m.Enqueue("p1", "5v5"); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// Maç bitince oyuncular serbest bırakılır ve arena temizlenir.
func TestMatchEndReleasesPlayersAndDestroysArena(t *testing.T) {
	prov := NewLocalProvisioner()
	asg := &recordingAssigner{}
	m := NewMatcher(nil, prov, asg)

	enqueueAll(t, m, "1v1", "p1", "p2")
	waitMatches(t, m, 1)

	a := prov.Get("arena-m1")
	if a == nil {
		t.Fatal("arena not found")
	}
	// Maçı hızlıca bitir: bir oyuncu ayrılsın.
	a.Push(arena.Command{PlayerID: "p2", Kind: arena.CmdLeave})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, released := asg.snapshot(); len(released) == 2 && prov.Count() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, released := asg.snapshot()
	t.Fatalf("cleanup incomplete: released=%v arenas=%d", released, prov.Count())
}
