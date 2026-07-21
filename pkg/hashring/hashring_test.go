package hashring

import (
	"fmt"
	"testing"
)

func keys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = fmt.Sprintf("player-%d", i)
	}
	return ks
}

// mapAll, her anahtarın sahibini döner.
func mapAll(r *Ring, ks []string) map[string]string {
	m := make(map[string]string, len(ks))
	for _, k := range ks {
		m[k] = r.Get(k)
	}
	return m
}

// Boş halka "" döner; tek düğüm tüm anahtarları alır.
func TestEmptyAndSingle(t *testing.T) {
	r := New(50)
	if got := r.Get("x"); got != "" {
		t.Fatalf("empty ring Get = %q, want empty", got)
	}
	r.Add("only")
	for _, k := range keys(100) {
		if got := r.Get(k); got != "only" {
			t.Fatalf("single node: Get(%s) = %q, want only", k, got)
		}
	}
}

// Deterministik: aynı anahtar hep aynı düğüme.
func TestDeterministic(t *testing.T) {
	r := New(100)
	r.Add("a", "b", "c")
	for _, k := range keys(500) {
		if r.Get(k) != r.Get(k) {
			t.Fatalf("Get(%s) not deterministic", k)
		}
	}
}

// Dağılım dengeli olmalı: 100 vnode ile her düğüm ortalamanın makul
// bandında (±%30) anahtar almalı.
func TestBalancedDistribution(t *testing.T) {
	r := New(200)
	nodes := []string{"s1", "s2", "s3", "s4"}
	r.Add(nodes...)

	const n = 40000
	counts := map[string]int{}
	for _, k := range keys(n) {
		counts[r.Get(k)]++
	}
	mean := n / len(nodes)
	for _, node := range nodes {
		lo, hi := int(float64(mean)*0.7), int(float64(mean)*1.3)
		if counts[node] < lo || counts[node] > hi {
			t.Fatalf("node %s got %d keys, want within [%d,%d] (mean %d)", node, counts[node], lo, hi, mean)
		}
	}
}

// ASIL GARANTİ — minimal remap: düğüm eklenince yalnız ~1/(N+1) anahtar
// taşınır ve taşınanların HEPSİ yeni düğüme gider (eski düğümler arası
// taşınma OLMAZ). Naif modulo'da bu oran ~1 olurdu.
func TestMinimalRemapOnAdd(t *testing.T) {
	r := New(200)
	r.Add("a", "b", "c")
	ks := keys(20000)
	before := mapAll(r, ks)

	r.Add("d")
	after := mapAll(r, ks)

	moved := 0
	for _, k := range ks {
		if before[k] != after[k] {
			moved++
			if after[k] != "d" {
				t.Fatalf("key %s moved %s->%s, but only moves TO new node 'd' are allowed",
					k, before[k], after[k])
			}
		}
	}
	frac := float64(moved) / float64(len(ks))
	// Beklenen ~1/4 = 0.25; geniş ama anlamlı bir bant.
	if frac < 0.15 || frac > 0.35 {
		t.Fatalf("moved fraction = %.3f, want ~0.25 (consistent hashing)", frac)
	}
}

// Düğüm çıkınca yalnız o düğümün anahtarları taşınır; diğerleri sabit.
func TestMinimalRemapOnRemove(t *testing.T) {
	r := New(200)
	r.Add("a", "b", "c", "d")
	ks := keys(20000)
	before := mapAll(r, ks)

	r.Remove("c")
	after := mapAll(r, ks)

	for _, k := range ks {
		if before[k] == "c" {
			if after[k] == "c" || after[k] == "" {
				t.Fatalf("key %s stayed on removed node (now %q)", k, after[k])
			}
		} else if before[k] != after[k] {
			t.Fatalf("key %s not on removed node moved %s->%s (must stay)", k, before[k], after[k])
		}
	}
}

// GetN: replika yerleşimi — n farklı düğüm, sırayla saat yönünde.
func TestGetNDistinctNodes(t *testing.T) {
	r := New(100)
	r.Add("a", "b", "c", "d", "e")
	for _, k := range keys(200) {
		got := r.GetN(k, 3)
		if len(got) != 3 {
			t.Fatalf("GetN(%s,3) = %v, want 3 nodes", k, got)
		}
		seen := map[string]bool{}
		for _, node := range got {
			if seen[node] {
				t.Fatalf("GetN(%s) returned duplicate node %s", k, node)
			}
			seen[node] = true
		}
		if got[0] != r.Get(k) {
			t.Fatalf("GetN[0] (%s) != Get (%s)", got[0], r.Get(k))
		}
	}
	// n > düğüm sayısı → tüm düğümler.
	if got := r.GetN("x", 99); len(got) != 5 {
		t.Fatalf("GetN with n>nodes = %d nodes, want 5", len(got))
	}
}

// Add idempotent, Remove olmayan düğümde no-op.
func TestAddRemoveEdgeCases(t *testing.T) {
	r := New(10)
	r.Add("a")
	n1 := len(r.ring)
	r.Add("a") // tekrar ekleme
	if len(r.ring) != n1 {
		t.Fatalf("duplicate Add changed ring size %d->%d", n1, len(r.ring))
	}
	r.Remove("yok") // olmayan
	r.Remove("a")
	if len(r.ring) != 0 || r.Get("x") != "" {
		t.Fatalf("ring not empty after removing all: size=%d", len(r.ring))
	}
}
