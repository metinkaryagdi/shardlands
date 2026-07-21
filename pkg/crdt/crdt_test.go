package crdt_test

import (
	"fmt"
	"math/rand"
	"testing"

	"shardlands/pkg/clock"
	"shardlands/pkg/crdt"
)

// ---- G-Counter temel ----

func TestGCounterValueIsSum(t *testing.T) {
	g := crdt.NewGCounter()
	g.Increment("a", 3)
	g.Increment("b", 5)
	g.Increment("a", 2) // a: 5
	if got := g.Value(); got != 10 {
		t.Fatalf("Value = %d, want 10", got)
	}
}

// mergedG, verilen sayaçların kopyalarını sırayla birleştirip yeni bir
// sayaç döner (girdileri değiştirmez).
func mergedG(gs ...*crdt.GCounter) *crdt.GCounter {
	out := crdt.NewGCounter()
	for _, g := range gs {
		out.Merge(g)
	}
	return out
}

// sameState, iki sayacı bileşen-bazlı karşılaştırır. Yok anahtar = 0
// (sıfır bileşen ile hiç görülmemiş bileşen semantik olarak eşittir).
func sameState(a, b *crdt.GCounter) bool {
	sa, sb := a.State(), b.State()
	for k, v := range sa {
		if sb[k] != v {
			return false
		}
	}
	for k, v := range sb {
		if sa[k] != v {
			return false
		}
	}
	return true
}

// Merge özellikleri: commutative, associative, idempotent — CRDT sözleşmesi.
func TestGCounterMergeProperties(t *testing.T) {
	mk := func(a, b, c uint64) *crdt.GCounter {
		g := crdt.NewGCounter()
		g.Increment("a", a)
		g.Increment("b", b)
		g.Increment("c", c)
		return g
	}
	x, y, z := mk(3, 0, 1), mk(1, 4, 0), mk(0, 2, 5)

	// Commutative: x⊔y = y⊔x
	if !sameState(mergedG(x, y), mergedG(y, x)) {
		t.Fatal("merge not commutative")
	}
	// Associative: (x⊔y)⊔z = x⊔(y⊔z)
	left := mergedG(mergedG(x, y), z)
	right := mergedG(x, mergedG(y, z))
	if !sameState(left, right) {
		t.Fatal("merge not associative")
	}
	// Idempotent: x⊔x = x, ve iki kez merge = bir kez.
	if !sameState(mergedG(x, x), x) {
		t.Fatal("merge not idempotent (x⊔x != x)")
	}
	once := mergedG(x, y)
	twice := mergedG(once, y)
	if !sameState(once, twice) {
		t.Fatal("re-merging same state changed result (not idempotent)")
	}
}

// Merge bileşen-bazlı MAX'tır ve bu tam olarak vector clock'un merge
// kuralıdır: aynı artışlardan kurulan bir G-Counter ile clock.Vector,
// merge sonrası AYNI haritaya sahip olmalı.
func TestGCounterMergeEqualsVectorClockMerge(t *testing.T) {
	g1, g2 := crdt.NewGCounter(), crdt.NewGCounter()
	v1, v2 := clock.NewVector(), clock.NewVector()

	// Aynı olayları hem sayaç hem saat olarak uygula.
	for i := 0; i < 3; i++ {
		g1.Increment("a", 1)
		v1.Tick("a")
	}
	g1.Increment("b", 1)
	v1.Tick("b")
	for i := 0; i < 2; i++ {
		g2.Increment("b", 1)
		v2.Tick("b")
	}
	g2.Increment("c", 1)
	v2.Tick("c")

	g1.Merge(g2)
	v1.Merge(v2)

	gs := g1.State()
	if len(gs) != len(v1) {
		t.Fatalf("state sizes differ: gcounter %v vs vector %v", gs, v1)
	}
	for node, c := range gs {
		if v1[node] != c {
			t.Fatalf("node %s: gcounter %d != vector %d (merge rules must match)", node, c, v1[node])
		}
	}
}

// Yakınsama: R replika bağımsız artırır; rastgele sıra ve TEKRARLI
// merge'lerden sonra hepsi aynı değere ve aynı duruma yakınsamalı.
func TestGCounterConvergence(t *testing.T) {
	const replicas, opsPer = 6, 200
	rng := rand.New(rand.NewSource(1))

	reps := make([]*crdt.GCounter, replicas)
	var total uint64
	for i := range reps {
		reps[i] = crdt.NewGCounter()
		id := fmt.Sprintf("r%d", i)
		for j := 0; j < opsPer; j++ {
			d := uint64(rng.Intn(3) + 1)
			reps[i].Increment(id, d)
			total += d
		}
	}

	// Kaotik gossip: rastgele iki replika, biri diğerinin ANLIK
	// kopyasını (Clone) merge eder; sıra ve tekrar bilinçli düzensiz.
	for round := 0; round < 500; round++ {
		a, b := rng.Intn(replicas), rng.Intn(replicas)
		reps[a].Merge(reps[b].Clone())
	}
	// Tam senkron: herkes herkesi alsın (yakınsamayı garantile).
	for i := range reps {
		for j := range reps {
			reps[i].Merge(reps[j].Clone())
		}
	}

	for i, r := range reps {
		if r.Value() != total {
			t.Fatalf("replica %d value = %d, want %d (not converged)", i, r.Value(), total)
		}
		if !sameState(r, reps[0]) {
			t.Fatalf("replica %d state diverges from replica 0", i)
		}
	}
}

// ---- PN-Counter ----

func TestPNCounterValue(t *testing.T) {
	c := crdt.NewPNCounter()
	c.Increment("a", 10)
	c.Decrement("a", 3)
	c.Decrement("b", 4)
	if got := c.Value(); got != 3 {
		t.Fatalf("Value = %d, want 3", got)
	}
	// Negatife inebilmeli.
	c.Decrement("b", 10)
	if got := c.Value(); got != -7 {
		t.Fatalf("Value = %d, want -7", got)
	}
}

func mergedPN(cs ...*crdt.PNCounter) *crdt.PNCounter {
	out := crdt.NewPNCounter()
	for _, c := range cs {
		out.Merge(c)
	}
	return out
}

// PN-Counter merge de commutative + idempotent olmalı; değer yakınsar.
func TestPNCounterMergeProperties(t *testing.T) {
	x := crdt.NewPNCounter()
	x.Increment("a", 5)
	x.Decrement("a", 1)
	y := crdt.NewPNCounter()
	y.Increment("b", 2)
	y.Decrement("b", 4)

	if mergedPN(x, y).Value() != mergedPN(y, x).Value() {
		t.Fatal("PN merge not commutative")
	}
	// Idempotent: tekrar merge değeri değiştirmemeli.
	once := mergedPN(x, y)
	before := once.Value()
	once.Merge(y.Clone())
	once.Merge(x.Clone())
	if once.Value() != before {
		t.Fatalf("re-merge changed value %d -> %d (not idempotent)", before, once.Value())
	}
	// x(5-1=4) + y(2-4=-2) = 2
	if got := mergedPN(x, y).Value(); got != 2 {
		t.Fatalf("merged value = %d, want 2", got)
	}
}

func TestPNCounterConvergence(t *testing.T) {
	const replicas = 5
	rng := rand.New(rand.NewSource(7))
	reps := make([]*crdt.PNCounter, replicas)
	var expected int64
	for i := range reps {
		reps[i] = crdt.NewPNCounter()
		id := fmt.Sprintf("r%d", i)
		for j := 0; j < 100; j++ {
			d := uint64(rng.Intn(3) + 1)
			if rng.Intn(2) == 0 {
				reps[i].Increment(id, d)
				expected += int64(d)
			} else {
				reps[i].Decrement(id, d)
				expected -= int64(d)
			}
		}
	}
	for round := 0; round < 400; round++ {
		a, b := rng.Intn(replicas), rng.Intn(replicas)
		reps[a].Merge(reps[b].Clone())
	}
	for i := range reps {
		for j := range reps {
			reps[i].Merge(reps[j].Clone())
		}
	}
	for i, r := range reps {
		if r.Value() != expected {
			t.Fatalf("replica %d value = %d, want %d", i, r.Value(), expected)
		}
	}
}
