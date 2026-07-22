package arena

import (
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkTick: tek arenanın tam simülasyon adımı (girdi boşaltma +
// fizik + çarpışma + snapshot).
func BenchmarkTick(b *testing.B) {
	a := New("bench", Mode2v2, []PlayerSpec{
		{ID: "a1", Team: 0}, {ID: "a2", Team: 0},
		{ID: "b1", Team: 1}, {ID: "b2", Team: 1},
	}, Options{})
	// Sürekli hareket + ateş baskısı.
	for _, id := range []string{"a1", "a2", "b1", "b2"} {
		a.Push(Command{PlayerID: id, Kind: CmdMove, Right: true, Down: true})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			a.Push(Command{PlayerID: "a1", Kind: CmdFire, AimX: 1, AimY: 0})
		}
		a.tickForBench()
	}
}

// tickForBench, bitiş kontrolü olmadan adım atar (benchmark maç
// bitiminde durmasın).
func (a *Arena) tickForBench() {
	a.drainInputs()
	a.step()
	a.tick++
	if a.tick%MatchTicks == 0 {
		for _, p := range a.players {
			p.health = MaxHealth // benchmark için canları tazele
		}
	}
}

// BenchmarkInputPush: oturum goroutine'lerinin KİLİTSİZ girdi kuyruğuna
// yazma maliyeti (arena sıcak yolu).
func BenchmarkInputPush(b *testing.B) {
	a := New("bench", Mode1v1, []PlayerSpec{{ID: "p1", Team: 0}, {ID: "p2", Team: 1}}, Options{})
	// Tek tüketici: kuyruğu boşaltıp doluluğu engeller.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				a.inputs.TryPop()
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := Command{PlayerID: "p1", Kind: CmdMove, Right: true}
		for pb.Next() {
			a.Push(c)
		}
	})
	b.StopTimer()
	close(stop)
	wg.Wait()
}

// ---- False sharing: çok sayıda arena, ayrı goroutine'ler, komşu sayaçlar ----
//
// Gerçek senaryo: tek makinede N arena instance'ı koşar ve her biri
// kendi sayacını (işlenen tick, isabet, düşen girdi) günceller. Sayaçlar
// bir dilimde YAN YANA durursa aynı önbellek satırına düşerler; farklı
// çekirdeklerdeki goroutine'ler birbirinin satırını sürekli geçersiz
// kılar (false sharing) — mantıksal paylaşım olmadan fiziksel çekişme.

const benchArenas = 8

// unpaddedCounter: 8 baytlık sayaç; dilimde 8 tanesi TEK 64B satıra sığar.
type unpaddedCounter struct {
	ticks atomic.Int64
}

// paddedCounter: sayaç + dolgu, her biri kendi önbellek satırında.
type paddedCounter struct {
	ticks atomic.Int64
	_     [56]byte // 64 - 8
}

func BenchmarkArenaCountersUnpadded(b *testing.B) {
	counters := make([]unpaddedCounter, benchArenas)
	runCounterBench(b, func(w, iters int) {
		for i := 0; i < iters; i++ {
			counters[w].ticks.Add(1)
		}
	})
}

func BenchmarkArenaCountersPadded(b *testing.B) {
	counters := make([]paddedCounter, benchArenas)
	runCounterBench(b, func(w, iters int) {
		for i := 0; i < iters; i++ {
			counters[w].ticks.Add(1)
		}
	})
}

// runCounterBench, benchArenas goroutine'i paralel çalıştırır; her biri
// KENDİ indeksindeki sayacı artırır (mantıksal paylaşım YOK).
func runCounterBench(b *testing.B, work func(worker, iters int)) {
	per := b.N / benchArenas
	if per == 0 {
		per = 1
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < benchArenas; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			work(w, per)
		}(w)
	}
	wg.Wait()
}
