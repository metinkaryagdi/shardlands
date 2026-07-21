// Package ringbuf, sıfırdan yazılmış bounded, lock-free MPSC (multi
// producer, single consumer) ring buffer sağlar. Tasarım, Dmitry
// Vyukov'un bounded MPMC kuyruğunun tek-tüketiciye sadeleştirilmiş
// halidir: her slot kendi sequence sayacını taşır ve doluluk/boşluk
// kararı bu sayaçla verilir — üretici ile tüketici birbirinin
// pozisyon sayacını hiç okumaz.
//
// Sequence protokolü (cap = C, slot i için başlangıç seq = i):
//
//	seq == pos      -> slot boş, pos'taki üretici yazabilir
//	seq == pos+1    -> slot dolu, pos'taki tüketici okuyabilir
//	seq == pos+C    -> slot boşaltıldı, bir sonraki turun üreticisini bekliyor
//
// Üreticiler enq sayacını CAS ile kapışır (lock yok); kazanan slotu
// yazar ve seq'i pos+1 yapıp mesajı "yayınlar". Tek tüketici deq
// sayacını yalnız kendisi ilerlettiği için CAS'e bile ihtiyaç duymaz.
//
// Dürüst dipnot: bu yapı pratikte non-blocking'dir ama teorik anlamda
// tamamen lock-free değildir — slotu CAS ile kapmış ama henüz seq'i
// yayınlamamış bir üretici (ör. OS tarafından askıya alındıysa),
// tüketicinin o slotu geçmesini geciktirir. Gerçek sistemlerdeki
// kullanımında (mailbox) bu pencere nanosaniyeler mertebesindedir.
package ringbuf

import "sync/atomic"

// cacheLinePad, sıcak sayaçları ayrı önbellek satırlarına iter. enq
// (üreticilerin CAS ile dövdüğü) ile deq (tüketicinin yazdığı) aynı
// satırda kalsaydı, her CAS tüketicinin satırını geçersiz kılar ve
// çekirdekler arası satır ping-pong'u (false sharing) başlardı.
type cacheLinePad [64]byte

type slot[T any] struct {
	seq atomic.Uint64
	val T
}

// MPSC, bounded lock-free multi-producer single-consumer kuyruktur.
// TryPush her goroutine'den çağrılabilir; TryPop yalnızca TEK bir
// tüketici goroutine'den çağrılmalıdır.
type MPSC[T any] struct {
	slots []slot[T]
	mask  uint64
	_     cacheLinePad
	enq   atomic.Uint64
	_     cacheLinePad
	deq   atomic.Uint64
	_     cacheLinePad
}

// New, en az capacity kapasiteli bir kuyruk yaratır. Kapasite maske
// aritmetiği için bir üst 2 kuvvetine yuvarlanır ve asla 2'nin altına
// inmez (gerçek değer Cap ile öğrenilir): cap==1'de "slot dolu"
// (seq==pos+1) ile "slot boşaltıldı, sonraki tura hazır" (seq==pos+cap)
// aynı seq değerine çakışır ve protokol sessizce bozulur — yazılmamış
// değer üzerine yazılır, kuyruk kilitlenir. capacity < 1 programlama
// hatasıdır ve panic'ler.
func New[T any](capacity int) *MPSC[T] {
	if capacity < 1 {
		panic("ringbuf: capacity must be >= 1")
	}
	n := 2
	for n < capacity {
		n <<= 1
	}
	q := &MPSC[T]{
		slots: make([]slot[T], n),
		mask:  uint64(n - 1),
	}
	for i := range q.slots {
		q.slots[i].seq.Store(uint64(i))
	}
	return q
}

// TryPush, v'yi kuyruğa eklemeyi dener; kuyruk doluysa false döner.
// Bloklamaz. Birden çok goroutine'den eşzamanlı çağrılabilir.
func (q *MPSC[T]) TryPush(v T) bool {
	for {
		pos := q.enq.Load()
		s := &q.slots[pos&q.mask]
		seq := s.seq.Load()
		switch dif := int64(seq) - int64(pos); {
		case dif == 0:
			// Slot boş; pozisyonu CAS ile kapmayı dene.
			if q.enq.CompareAndSwap(pos, pos+1) {
				s.val = v
				s.seq.Store(pos + 1) // yayınla: tüketici artık görebilir
				return true
			}
			// CAS'i başka üretici kazandı; taze pos ile tekrar dene.
		case dif < 0:
			// Slot hâlâ önceki turun değerini taşıyor: kuyruk dolu.
			return false
		default:
			// Başka üretici slotu kaptı, enq ilerledi; tekrar oku.
		}
	}
}

// TryPop, sıradaki elemanı döndürür; kuyruk boşsa ok=false. Bloklamaz.
// YALNIZCA tek bir tüketici goroutine'den çağrılmalıdır.
func (q *MPSC[T]) TryPop() (v T, ok bool) {
	pos := q.deq.Load()
	s := &q.slots[pos&q.mask]
	seq := s.seq.Load()
	if int64(seq)-int64(pos+1) < 0 {
		// Boş — ya da üretici slotu kaptı ama değeri henüz yayınlamadı;
		// iki durumda da şimdilik alınacak bir şey yok.
		return v, false
	}
	v = s.val
	var zero T
	s.val = zero                            // referansları bırak ki GC toplayabilsin
	s.seq.Store(pos + uint64(len(q.slots))) // slotu bir sonraki tura aç
	q.deq.Store(pos + 1)
	return v, true
}

// Len, kuyruktaki yaklaşık eleman sayısıdır (eşzamanlı push/pop
// sırasında anlık bir gözlemdir; kesinlik gerekiyorsa dışarıdan sayın).
func (q *MPSC[T]) Len() int {
	e, d := q.enq.Load(), q.deq.Load()
	if e <= d {
		return 0
	}
	n := int(e - d)
	if n > len(q.slots) {
		n = len(q.slots)
	}
	return n
}

// Cap, gerçek (yuvarlanmış) kapasitedir.
func (q *MPSC[T]) Cap() int { return len(q.slots) }
