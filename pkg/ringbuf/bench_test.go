package ringbuf_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"shardlands/pkg/ringbuf"
)

// benchMPSC: b.N elemanı paralel üreticilerden tek tüketiciye akıtır.
func benchMPSC(b *testing.B, push func(int) bool, pop func() (int, bool)) {
	b.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	n := b.N
	b.ResetTimer()
	go func() {
		defer wg.Done()
		for got := 0; got < n; {
			if _, ok := pop(); ok {
				got++
			} else {
				runtime.Gosched()
			}
		}
	}()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for !push(1) {
				runtime.Gosched()
			}
		}
	})
	wg.Wait()
}

func BenchmarkRingbufMPSC(b *testing.B) {
	q := ringbuf.New[int](1024)
	benchMPSC(b, q.TryPush, q.TryPop)
}

// Aynı iş yükü, Go kanalıyla (mailbox'ın eski implementasyonu).
func BenchmarkChannelMPSC(b *testing.B) {
	ch := make(chan int, 1024)
	benchMPSC(b,
		func(v int) bool {
			select {
			case ch <- v:
				return true
			default:
				return false
			}
		},
		func() (int, bool) {
			select {
			case v := <-ch:
				return v, true
			default:
				return 0, false
			}
		})
}

// unpaddedMPSC: ringbuf.MPSC ile birebir aynı algoritma, ama enq/deq
// sayaçları arasında cache-line dolgusu YOK. False sharing'in maliyetini
// ölçmek için yalnızca benchmark'ta yaşar.
type unpaddedMPSC struct {
	slots []unpaddedSlot
	mask  uint64
	enq   atomic.Uint64 // dolgu yok: enq ve deq büyük ihtimalle aynı cache satırında
	deq   atomic.Uint64
}

type unpaddedSlot struct {
	seq atomic.Uint64
	val int
}

func newUnpadded(capacity int) *unpaddedMPSC {
	n := 1
	for n < capacity {
		n <<= 1
	}
	q := &unpaddedMPSC{slots: make([]unpaddedSlot, n), mask: uint64(n - 1)}
	for i := range q.slots {
		q.slots[i].seq.Store(uint64(i))
	}
	return q
}

func (q *unpaddedMPSC) tryPush(v int) bool {
	for {
		pos := q.enq.Load()
		s := &q.slots[pos&q.mask]
		seq := s.seq.Load()
		switch dif := int64(seq) - int64(pos); {
		case dif == 0:
			if q.enq.CompareAndSwap(pos, pos+1) {
				s.val = v
				s.seq.Store(pos + 1)
				return true
			}
		case dif < 0:
			return false
		}
	}
}

func (q *unpaddedMPSC) tryPop() (int, bool) {
	pos := q.deq.Load()
	s := &q.slots[pos&q.mask]
	if int64(s.seq.Load())-int64(pos+1) < 0 {
		return 0, false
	}
	v := s.val
	s.seq.Store(pos + uint64(len(q.slots)))
	q.deq.Store(pos + 1)
	return v, true
}

func BenchmarkUnpaddedMPSC(b *testing.B) {
	q := newUnpadded(1024)
	benchMPSC(b, q.tryPush, q.tryPop)
}
