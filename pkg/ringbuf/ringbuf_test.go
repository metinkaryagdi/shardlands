package ringbuf_test

import (
	"math/rand"
	"runtime"
	"testing"
	"time"

	"shardlands/pkg/ringbuf"
)

// Tek üreticiyle FIFO sırası, kapasitenin çok üzerinde eleman
// akıtılarak (sarma/wraparound dahil) korunmalı.
func TestFIFOWithWraparound(t *testing.T) {
	q := ringbuf.New[int](4)
	next := 0 // beklenen sıradaki değer
	for pushed := 0; pushed < 100; {
		// Dolana kadar it, sonra hepsini çek: her turda sarma yaşanır.
		for q.TryPush(pushed) {
			pushed++
		}
		for {
			v, ok := q.TryPop()
			if !ok {
				break
			}
			if v != next {
				t.Fatalf("popped %d, want %d (FIFO broken)", v, next)
			}
			next++
		}
	}
	if next != 100 {
		t.Fatalf("popped %d items, want 100", next)
	}
}

func TestFullAndEmpty(t *testing.T) {
	q := ringbuf.New[string](2)
	if _, ok := q.TryPop(); ok {
		t.Fatal("pop on empty queue must fail")
	}
	if !q.TryPush("a") || !q.TryPush("b") {
		t.Fatal("pushes within capacity must succeed")
	}
	if q.TryPush("c") {
		t.Fatal("push on full queue must fail")
	}
	if v, ok := q.TryPop(); !ok || v != "a" {
		t.Fatalf("pop = %q,%v, want a,true", v, ok)
	}
	if !q.TryPush("c") {
		t.Fatal("push after pop must succeed (slot freed)")
	}
}

func TestCapacityRounding(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{1, 2}, {2, 2}, {3, 4}, {10, 16}, {64, 64}, {100, 128},
	} {
		if got := ringbuf.New[int](c.in).Cap(); got != c.want {
			t.Errorf("New(%d).Cap() = %d, want %d", c.in, got, c.want)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("New(0) must panic")
		}
	}()
	ringbuf.New[int](0)
}

// cap==1 isteği 2'ye yuvarlanmalı ve küçük kapasitede uzun push/pop
// döngüleri protokolü bozmamalı (cap-1 çakışması regresyon testi).
func TestMinCapacityCycle(t *testing.T) {
	q := ringbuf.New[int](1)
	if q.Cap() != 2 {
		t.Fatalf("Cap = %d, want 2 (minimum)", q.Cap())
	}
	next := 0
	for i := 0; i < 100; i++ {
		if !q.TryPush(2*i) || !q.TryPush(2*i+1) {
			t.Fatalf("round %d: pushes within capacity must succeed", i)
		}
		if q.TryPush(-1) {
			t.Fatalf("round %d: push on full queue must fail", i)
		}
		for j := 0; j < 2; j++ {
			v, ok := q.TryPop()
			if !ok || v != next {
				t.Fatalf("round %d: pop = %d,%v, want %d,true", i, v, ok, next)
			}
			next++
		}
	}
}

func TestLen(t *testing.T) {
	q := ringbuf.New[int](8)
	if q.Len() != 0 {
		t.Fatalf("empty Len = %d, want 0", q.Len())
	}
	q.TryPush(1)
	q.TryPush(2)
	q.TryPush(3)
	if q.Len() != 3 {
		t.Fatalf("Len = %d, want 3", q.Len())
	}
	q.TryPop()
	if q.Len() != 2 {
		t.Fatalf("Len after pop = %d, want 2", q.Len())
	}
}

// item, üreticiyi ve o üreticinin kaçıncı mesajı olduğunu kodlar;
// tüketici tarafında kayıp/çoğaltma/sıra bozulması tespiti için.
type item struct {
	producer int
	seq      int
}

// mpscInvariants: producers üretici, her biri perProducer eleman iter;
// tek tüketici hepsini toplar. Değişmezler: hiç kayıp yok, hiç kopya
// yok, her üreticinin KENDİ mesajları sıralı (global sıra garanti
// edilmez, edilmemeli).
func mpscInvariants(t *testing.T, capacity, producers, perProducer int, chaotic bool) {
	t.Helper()
	q := ringbuf.New[item](capacity)
	total := producers * perProducer

	for p := 0; p < producers; p++ {
		go func(p int) {
			rng := rand.New(rand.NewSource(int64(p)))
			for i := 0; i < perProducer; i++ {
				for !q.TryPush(item{producer: p, seq: i}) {
					runtime.Gosched() // dolu: tüketiciye nefes ver
				}
				if chaotic && rng.Intn(50) == 0 {
					time.Sleep(time.Duration(rng.Intn(200)) * time.Microsecond)
				}
			}
		}(p)
	}

	nextSeq := make([]int, producers) // üretici başına beklenen sıradaki seq
	rng := rand.New(rand.NewSource(99))
	deadline := time.Now().Add(30 * time.Second)
	for got := 0; got < total; {
		v, ok := q.TryPop()
		if !ok {
			if time.Now().After(deadline) {
				t.Fatalf("timeout: consumed %d/%d", got, total)
			}
			runtime.Gosched()
			continue
		}
		if v.seq != nextSeq[v.producer] {
			t.Fatalf("producer %d: got seq %d, want %d (per-producer order broken)",
				v.producer, v.seq, nextSeq[v.producer])
		}
		nextSeq[v.producer]++
		got++
		if chaotic && rng.Intn(200) == 0 {
			time.Sleep(time.Duration(rng.Intn(300)) * time.Microsecond)
		}
	}
	for p, n := range nextSeq {
		if n != perProducer {
			t.Fatalf("producer %d: consumed %d, want %d", p, n, perProducer)
		}
	}
}

func TestMPSCStress(t *testing.T) {
	mpscInvariants(t, 1024, 8, 20000, false)
}

// Küçük kapasite + rastgele duraklamalar: sarma ve dolu-kuyruk yolları
// sürekli tetiklenir (mini chaos testi).
func TestMPSCChaosSmallBuffer(t *testing.T) {
	mpscInvariants(t, 4, 6, 2000, true)
}
