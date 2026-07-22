package raftstore

import (
	"fmt"
	"os"
	"testing"
	"time"

	"shardlands/pkg/raft"
)

func tmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-raftstore-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func mustOpen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, false) // testlerde fsync kapalı (hız)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func entries(terms ...uint64) []raft.Entry {
	out := make([]raft.Entry, len(terms))
	for i, term := range terms {
		out[i] = raft.Entry{Term: term, Cmd: []byte(fmt.Sprintf("cmd-%d-%d", i, term))}
	}
	return out
}

func sameLog(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || string(a[i].Cmd) != string(b[i].Cmd) {
			return false
		}
	}
	return true
}

// Boş depo sıfır durum döner; kaydedilen durum aynen geri okunur.
func TestSaveLoadRoundTrip(t *testing.T) {
	dir := tmpDir(t)
	s := mustOpen(t, dir)
	defer s.Close()

	hs, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 0 || hs.VotedFor != "" || len(hs.Log) != 0 {
		t.Fatalf("empty store = %+v, want zero state", hs)
	}

	want := raft.HardState{Term: 7, VotedFor: "n2", Log: entries(1, 1, 3, 7)}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != 7 || got.VotedFor != "n2" || !sameLog(got.Log, want.Log) {
		t.Fatalf("loaded = %+v, want %+v", got, want)
	}
}

// Log kısalırsa fazlalık kayıtlar silinmeli (truncate).
func TestTruncateShrinksLog(t *testing.T) {
	dir := tmpDir(t)
	s := mustOpen(t, dir)
	defer s.Close()

	if err := s.Save(raft.HardState{Term: 3, Log: entries(1, 1, 2, 3, 3)}); err != nil {
		t.Fatal(err)
	}
	short := raft.HardState{Term: 3, Log: entries(1, 1, 2)}
	if err := s.Save(short); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !sameLog(got.Log, short.Log) {
		t.Fatalf("after truncate log = %d entries, want %d", len(got.Log), len(short.Log))
	}
}

// Çakışma: aynı uzunlukta ama farklı kuyruk — değişen sonek yazılmalı.
func TestConflictReplacesSuffix(t *testing.T) {
	dir := tmpDir(t)
	s := mustOpen(t, dir)
	defer s.Close()

	if err := s.Save(raft.HardState{Term: 2, Log: entries(1, 2, 2)}); err != nil {
		t.Fatal(err)
	}
	replaced := raft.HardState{Term: 5, VotedFor: "n1", Log: entries(1, 5, 5)}
	if err := s.Save(replaced); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !sameLog(got.Log, replaced.Log) {
		t.Fatalf("conflict suffix not replaced: %+v", got.Log)
	}
	if got.Term != 5 || got.VotedFor != "n1" {
		t.Fatalf("meta = %d/%s, want 5/n1", got.Term, got.VotedFor)
	}
}

// Kapat-aç: durum diskten geri gelir ve sonraki Save doğru diff yapar.
func TestReopenRestoresAndDiffs(t *testing.T) {
	dir := tmpDir(t)
	s := mustOpen(t, dir)
	first := raft.HardState{Term: 4, VotedFor: "n3", Log: entries(1, 2, 4)}
	if err := s.Save(first); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != 4 || got.VotedFor != "n3" || !sameLog(got.Log, first.Log) {
		t.Fatalf("reopen lost state: %+v", got)
	}

	// Aynanın doğru kurulduğunu göster: uzatma sonrası tam log okunmalı.
	extended := raft.HardState{Term: 5, VotedFor: "n3", Log: entries(1, 2, 4, 5, 5)}
	if err := s2.Save(extended); err != nil {
		t.Fatal(err)
	}
	got2, _ := s2.Load()
	if !sameLog(got2.Log, extended.Log) {
		t.Fatalf("after reopen+extend log = %+v", got2.Log)
	}
}

// Entegrasyon: gerçek bir Raft düğümü bu depoyla çalışır; durdurup aynı
// dizinle yeniden açınca dönem ve log korunur (persist-then-respond'un
// kalıcı karşılığı).
func TestRaftNodePersistsAcrossRestart(t *testing.T) {
	dir := tmpDir(t)
	store := mustOpen(t, dir)

	nw := raft.NewNetwork()
	node, err := raft.NewNode(raft.Config{
		ID: "solo", Peers: nil, // tek düğüm: kendi oyuyla lider olur
		Transport:          nw.Transport("solo"),
		Storage:            store,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
		TickInterval:       5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	nw.Register("solo", node)

	// Lider olmasını bekle, sonra birkaç komut öner.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, isLeader := node.Status(); isLeader {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, isLeader := node.Status(); !isLeader {
		t.Fatal("single node never became leader")
	}
	for i := 0; i < 3; i++ {
		if _, _, ok := node.Propose([]byte(fmt.Sprintf("op-%d", i))); !ok {
			t.Fatalf("propose %d rejected", i)
		}
	}
	term, _ := node.Status()
	node.Stop()
	store.Close()

	// Aynı dizinle yeniden aç: log ve dönem korunmuş olmalı.
	store2 := mustOpen(t, dir)
	defer store2.Close()
	hs, err := store2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term < term {
		t.Fatalf("persisted term %d < pre-restart term %d", hs.Term, term)
	}
	if len(hs.Log) != 3 {
		t.Fatalf("persisted log = %d entries, want 3", len(hs.Log))
	}
	for i, e := range hs.Log {
		if want := fmt.Sprintf("op-%d", i); string(e.Cmd) != want {
			t.Fatalf("entry %d = %q, want %q", i, e.Cmd, want)
		}
	}
}
