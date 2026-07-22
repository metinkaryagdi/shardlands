package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Patlama kapasitesi kadar geçer, fazlası reddedilir.
func TestBurstThenDeny(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := New(Options{Rate: 10, Burst: 3, Now: clk.Now})

	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("call %d denied within burst", i)
		}
	}
	if b.Allow() {
		t.Fatal("call beyond burst allowed")
	}
}

// Zaman geçtikçe jeton dolar (tembel doldurma).
func TestRefillOverTime(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := New(Options{Rate: 10, Burst: 5, Now: clk.Now}) // 10 jeton/sn

	for i := 0; i < 5; i++ {
		b.Allow()
	}
	if b.Allow() {
		t.Fatal("bucket should be empty")
	}

	clk.Advance(300 * time.Millisecond) // ~3 jeton
	allowed := 0
	for i := 0; i < 5; i++ {
		if b.Allow() {
			allowed++
		}
	}
	if allowed < 2 || allowed > 3 {
		t.Fatalf("after 300ms allowed %d, want 2-3", allowed)
	}
}

// Doldurma tavanı burst'ü aşmaz (uzun boşluk sonrası sınırsız patlama yok).
func TestRefillCapsAtBurst(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := New(Options{Rate: 100, Burst: 4, Now: clk.Now})
	for i := 0; i < 4; i++ {
		b.Allow()
	}
	clk.Advance(10 * time.Second) // teorik 1000 jeton

	allowed := 0
	for i := 0; i < 20; i++ {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 4 {
		t.Fatalf("allowed %d after long idle, want 4 (capped at burst)", allowed)
	}
}

// AllowN ya hep ya hiç: yetmezse jeton harcamaz.
func TestAllowNAtomic(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := New(Options{Rate: 1, Burst: 5, Now: clk.Now})
	if b.AllowN(10) {
		t.Fatal("AllowN(10) with burst 5 must fail")
	}
	if got := b.Tokens(); got != 5 {
		t.Fatalf("tokens = %v after failed AllowN, want 5 (nothing spent)", got)
	}
	if !b.AllowN(5) {
		t.Fatal("AllowN(5) should succeed")
	}
}

// Anahtar başına yalıtım: bir istemcinin taşkınlığı diğerini etkilemez.
func TestKeyedIsolation(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	k := NewKeyed(Options{Rate: 1, Burst: 2, Now: clk.Now})

	if !k.Allow("a") || !k.Allow("a") {
		t.Fatal("a's burst denied")
	}
	if k.Allow("a") {
		t.Fatal("a exceeded its burst but was allowed")
	}
	// b etkilenmemeli.
	if !k.Allow("b") || !k.Allow("b") {
		t.Fatal("b was affected by a's flooding (isolation broken)")
	}
}

// Cleanup, boşta kalan kovaları siler (bellek sızıntısı olmasın).
func TestKeyedCleanup(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	k := NewKeyed(Options{Rate: 5, Burst: 5, Now: clk.Now})
	k.Allow("eski")
	clk.Advance(2 * time.Minute)
	k.Allow("yeni")

	if got := k.Size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
	if removed := k.Cleanup(time.Minute); removed != 1 {
		t.Fatalf("cleanup removed %d, want 1", removed)
	}
	if got := k.Size(); got != 1 {
		t.Fatalf("size after cleanup = %d, want 1", got)
	}
}

// Eşzamanlı kullanımda toplam izin, burst'ü aşmamalı (-race ile anlamlı).
func TestConcurrentAllowRespectsBurst(t *testing.T) {
	clk := &fakeClock{t: time.Now()} // zaman durdurulmuş: doldurma yok
	b := New(Options{Rate: 1000, Burst: 50, Now: clk.Now})

	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("allowed %d concurrent calls, want exactly 50 (burst)", got)
	}
}
