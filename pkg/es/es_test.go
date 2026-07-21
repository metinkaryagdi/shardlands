package es

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// storage testlerindeki testDir ile aynı gerekçe: Windows'ta
// delete-pending yüzünden en-iyi-çaba temizlik.
func testDir(tb testing.TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "shardlands-es-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func mustOpen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func ev(typ, data string) EventData { return EventData{Type: typ, Data: []byte(data)} }

func TestAppendAndReadRoundTrip(t *testing.T) {
	s := mustOpen(t, testDir(t))

	if _, err := s.Append("player-1", 0, ev("Joined", `{"n":"a"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("chat", 0, ev("Said", `{"t":"selam"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("player-1", 1, ev("Moved", `{}`)); err != nil {
		t.Fatal(err)
	}

	all, err := s.ReadAll(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("ReadAll = %d events, want 3", len(all))
	}
	for i, e := range all {
		if e.Global != uint64(i+1) {
			t.Fatalf("global[%d] = %d, want %d (dense, ordered)", i, e.Global, i+1)
		}
	}
	if all[0].Type != "Joined" || all[1].Type != "Said" || all[2].Type != "Moved" {
		t.Fatalf("order = %s,%s,%s", all[0].Type, all[1].Type, all[2].Type)
	}

	p1, err := s.ReadStream("player-1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(p1) != 2 || p1[0].Seq != 1 || p1[1].Seq != 2 || p1[1].Type != "Moved" {
		t.Fatalf("stream player-1 = %+v", p1)
	}
	if string(p1[0].Data) != `{"n":"a"}` {
		t.Fatalf("data round trip failed: %s", p1[0].Data)
	}
	if got := s.Version("player-1"); got != 2 {
		t.Fatalf("version = %d, want 2", got)
	}
	if got := s.LastGlobal(); got != 3 {
		t.Fatalf("lastGlobal = %d, want 3", got)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	s := mustOpen(t, testDir(t))

	if _, err := s.Append("acc", 0, ev("A", `1`)); err != nil {
		t.Fatal(err)
	}
	// Yanlış beklenti: başka bir yazar araya girmiş gibi.
	if _, err := s.Append("acc", 0, ev("B", `2`)); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale expected: %v, want ErrVersionConflict", err)
	}
	// Doğru beklenti ve AnyVersion çalışmalı.
	if _, err := s.Append("acc", 1, ev("B", `2`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("acc", AnyVersion, ev("C", `3`)); err != nil {
		t.Fatal(err)
	}
	if got := s.Version("acc"); got != 3 {
		t.Fatalf("version = %d, want 3", got)
	}
	// Çakışma mağazayı DEĞİŞTİRMEMİŞ olmalı.
	evs, _ := s.ReadStream("acc", 0, 0)
	if len(evs) != 3 {
		t.Fatalf("events = %d, want 3 (conflict must not append)", len(evs))
	}
}

// Çoklu event'li Append tek batch'tir: seq'ler bitişik, batch ortasından
// ReadAll doğru çalışır.
func TestMultiEventBatch(t *testing.T) {
	s := mustOpen(t, testDir(t))

	out, err := s.Append("trade-1", 0, ev("Offered", `1`), ev("Accepted", `2`), ev("Completed", `3`))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 || out[0].Global != 1 || out[2].Global != 3 || out[2].Seq != 3 {
		t.Fatalf("returned events = %+v", out)
	}

	// Batch ortasından okuma: from=2 → Accepted, Completed.
	mid, err := s.ReadAll(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(mid) != 2 || mid[0].Type != "Accepted" || mid[1].Type != "Completed" {
		t.Fatalf("ReadAll(2) = %+v", mid)
	}
	// Limit çalışmalı.
	one, _ := s.ReadAll(0, 1)
	if len(one) != 1 || one[0].Type != "Offered" {
		t.Fatalf("ReadAll limit = %+v", one)
	}
}

// Kapat-aç: indeks log'dan yeniden kurulmalı; versiyonlar, global sıra
// ve içerik aynı kalmalı; append kaldığı yerden devam etmeli.
func TestReopenRebuildsIndex(t *testing.T) {
	dir := testDir(t)
	s := mustOpen(t, dir)
	s.Append("a", 0, ev("A1", `1`), ev("A2", `2`))
	s.Append("b", 0, ev("B1", `3`))
	s.Append("a", 2, ev("A3", `4`))
	before, _ := s.ReadAll(0, 0)
	s.Close()

	s2 := mustOpen(t, dir)
	after, err := s2.ReadAll(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%+v", after) != fmt.Sprintf("%+v", before) {
		t.Fatalf("reopen changed log:\nbefore %+v\nafter  %+v", before, after)
	}
	if s2.Version("a") != 3 || s2.Version("b") != 1 || s2.LastGlobal() != 4 {
		t.Fatalf("rebuilt state: a=%d b=%d g=%d", s2.Version("a"), s2.Version("b"), s2.LastGlobal())
	}
	// Devam eden append doğru seq/global almalı.
	out, err := s2.Append("a", 3, ev("A4", `5`))
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Seq != 4 || out[0].Global != 5 {
		t.Fatalf("continued append = %+v", out[0])
	}
}

// Projection deseni: checkpoint'ten oku → işle → notify bekle → tekrar.
func TestSubscribeCatchUp(t *testing.T) {
	s := mustOpen(t, testDir(t))
	s.Append("x", AnyVersion, ev("E1", `1`))

	ch, cancel := s.Subscribe()
	defer cancel()

	// Catch-up: mevcutları oku.
	evs, _ := s.ReadAll(1, 0)
	if len(evs) != 1 {
		t.Fatalf("catch-up = %d, want 1", len(evs))
	}
	checkpoint := evs[len(evs)-1].Global

	// Yeni append sinyal üretmeli.
	s.Append("x", AnyVersion, ev("E2", `2`))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no notify after append")
	}
	fresh, _ := s.ReadAll(checkpoint+1, 0)
	if len(fresh) != 1 || fresh[0].Type != "E2" {
		t.Fatalf("incremental read = %+v, want only E2", fresh)
	}

	// cancel sonrası sinyal gelmemeli.
	cancel()
	s.Append("x", AnyVersion, ev("E3", `3`))
	select {
	case <-ch:
		t.Fatal("signal after unsubscribe")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAppendValidation(t *testing.T) {
	s := mustOpen(t, testDir(t))
	if _, err := s.Append("", 0, ev("A", `1`)); err == nil {
		t.Fatal("empty stream must be rejected")
	}
	if _, err := s.Append("x", 0); err == nil {
		t.Fatal("zero events must be rejected")
	}
}
