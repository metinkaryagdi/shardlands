package clock

import (
	"sync"
	"testing"
)

// Lamport: gönderim damgası < alım damgası (nedensellik korunur).
func TestLamportSendReceive(t *testing.T) {
	var a, b Lamport
	for i := 0; i < 100; i++ {
		send := a.Tick()
		recv := b.Observe(send)
		if recv <= send {
			t.Fatalf("recv %d must be > send %d", recv, send)
		}
	}
}

// Lamport: tek goroutine'in gördüğü damgalar kesin artan olmalı;
// eşzamanlı kullanım -race altında güvenli olmalı.
func TestLamportConcurrentMonotonic(t *testing.T) {
	var l Lamport
	var remote Lamport
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := uint64(0)
			for i := 0; i < 1000; i++ {
				var ts uint64
				if i%3 == 0 {
					ts = l.Observe(remote.Tick())
				} else {
					ts = l.Tick()
				}
				if ts <= last {
					t.Errorf("timestamps not strictly increasing: %d after %d", ts, last)
					return
				}
				last = ts
			}
		}()
	}
	wg.Wait()
	if l.Now() < 8*1000 {
		t.Fatalf("final clock %d, want >= 8000 (no lost ticks)", l.Now())
	}
}

// Vector: mesaj zinciri nedenselliği Before/After olarak görünmeli.
func TestVectorHappensBefore(t *testing.T) {
	a, b := NewVector(), NewVector()

	a.Tick("a")
	stamp := a.Clone() // a olayının damgası (mesajla gönderilir)

	b.Merge(stamp) // b mesajı alır
	b.Tick("b")

	if got := stamp.Compare(b); got != Before {
		t.Fatalf("send.Compare(recv) = %v, want before", got)
	}
	if got := b.Compare(stamp); got != After {
		t.Fatalf("recv.Compare(send) = %v, want after", got)
	}
}

// Vector: bağımsız olaylar Concurrent olarak TESPİT edilmeli (Lamport
// bunu yapamaz — vector clock'un varlık sebebi).
func TestVectorConcurrent(t *testing.T) {
	a, b := NewVector(), NewVector()
	a.Tick("a")
	b.Tick("b")
	if got := a.Compare(b); got != Concurrent {
		t.Fatalf("independent events = %v, want concurrent", got)
	}
	if got := b.Compare(a); got != Concurrent {
		t.Fatalf("symmetry: %v, want concurrent", got)
	}
}

func TestVectorEqualAndClone(t *testing.T) {
	a := NewVector()
	a.Tick("x")
	a.Tick("y")

	c := a.Clone()
	if got := a.Compare(c); got != Equal {
		t.Fatalf("clone compare = %v, want equal", got)
	}
	c.Tick("x") // klon bağımsız olmalı
	if got := a.Compare(c); got != Before {
		t.Fatalf("after clone tick = %v, want before", got)
	}
	if a["x"] != 1 {
		t.Fatalf("original mutated by clone: x=%d", a["x"])
	}
}

// Görünmeyen düğüm 0 sayılır: tek taraflı bilinen düğümler doğru
// karşılaştırılmalı.
func TestVectorMissingEntries(t *testing.T) {
	a := Vector{"n1": 1}
	b := Vector{"n1": 1, "n2": 1}
	if got := a.Compare(b); got != Before {
		t.Fatalf("subset = %v, want before", got)
	}
	if got := b.Compare(a); got != After {
		t.Fatalf("superset = %v, want after", got)
	}
}

// Üç düğümlü senaryo: zincirleme nedensellik ve çapraz eşzamanlılık
// birlikte — a1 → b1 → c1, a2 bağımsız.
func TestVectorThreeNodeScenario(t *testing.T) {
	a, b, c := NewVector(), NewVector(), NewVector()

	a.Tick("a")
	a1 := a.Clone()

	b.Merge(a1)
	b.Tick("b")
	b1 := b.Clone()

	c.Merge(b1)
	c.Tick("c")
	c1 := c.Clone()

	a.Tick("a") // a'nın b/c'den habersiz ikinci olayı
	a2 := a.Clone()

	if got := a1.Compare(c1); got != Before {
		t.Fatalf("a1 vs c1 = %v, want before (transitivity)", got)
	}
	if got := a2.Compare(b1); got != Concurrent {
		t.Fatalf("a2 vs b1 = %v, want concurrent", got)
	}
	if got := a2.Compare(c1); got != Concurrent {
		t.Fatalf("a2 vs c1 = %v, want concurrent", got)
	}
}
