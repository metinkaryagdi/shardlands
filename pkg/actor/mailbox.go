package actor

import (
	"sync"

	"shardlands/pkg/ringbuf"
)

// envelope, mesajı göndereniyle birlikte taşır.
type envelope struct {
	msg    any
	sender *Ref
}

// poisonPill, mailbox'taki sırasını bekleyen nazik durdurma mesajıdır
// (Ref.Poison). Ref.Stop ise kontrol kuyruğundan gider ve kuyruktaki
// kullanıcı mesajlarını beklemez.
type poisonPill struct{}

// OverflowPolicy, mailbox dolduğunda gönderenin ne yapacağını belirler.
type OverflowPolicy int

const (
	// Block: mailbox'ta yer açılana (veya aktör durana) kadar gönderen
	// bloklanır. Doğal backpressure sağlar.
	Block OverflowPolicy = iota
	// DropNewest: yer yoksa yeni mesaj atılır ve dead letter sayılır.
	// Gecikmeye duyarlı, kaybı tolere eden akışlar için.
	DropNewest
)

// userMailbox, kullanıcı mesajları için lock-free MPSC ring buffer +
// kanal tabanlı uyandırma sinyalleridir. Ring buffer poll tabanlı olduğu
// için bloklama iki cap-1 sinyal kanalıyla kurulur:
//
//	notify: üretici push'tan SONRA sinyaller; boş kuyrukta bekleyen
//	        tüketiciyi uyandırır.
//	space:  tüketici pop'tan SONRA sinyaller; dolu kuyrukta (Block
//	        politikası) bekleyen üreticiyi uyandırır.
//
// Sinyaller birleştirilir (coalesced): kanal doluysa yeni sinyal
// düşer. Bu kayıp değildir — sinyal "durum değişti, tekrar dene"
// demektir, mesajın kendisi ring buffer'dadır. Uyanan taraf her zaman
// bir try döngüsüyle yeniden dener.
type userMailbox struct {
	q      *ringbuf.MPSC[envelope]
	notify chan struct{}
	space  chan struct{}
}

func newUserMailbox(capacity int) *userMailbox {
	return &userMailbox{
		q:      ringbuf.New[envelope](capacity),
		notify: make(chan struct{}, 1),
		space:  make(chan struct{}, 1),
	}
}

func (m *userMailbox) tryPush(env envelope) bool {
	if !m.q.TryPush(env) {
		return false
	}
	signal(m.notify)
	return true
}

// tryPop yalnızca aktörün kendi goroutine'inden çağrılır (single consumer).
func (m *userMailbox) tryPop() (envelope, bool) {
	env, ok := m.q.TryPop()
	if ok {
		signal(m.space)
	}
	return env, ok
}

// wait: "kuyruğa mesaj geldi" sinyali (tüketici için).
func (m *userMailbox) wait() <-chan struct{} { return m.notify }

// spaceFreed: "kuyrukta yer açıldı" sinyali (Block'taki üretici için).
func (m *userMailbox) spaceFreed() <-chan struct{} { return m.space }

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// ctrlQueue, kontrol mesajları için unbounded MPSC kuyruktur. Unbounded
// olması bilinçli: kontrol mesajları (childStopped, escalation, stop)
// framework içinden üretilir, kaybolmaları veya göndereni kilitlemeleri
// deadlock'a yol açar. Kullanıcı mesajları ise bounded mailbox'tadır.
type ctrlQueue struct {
	mu     sync.Mutex
	items  []any
	notify chan struct{} // cap 1: "kuyrukta bir şeyler var" sinyali
}

func newCtrlQueue() *ctrlQueue {
	return &ctrlQueue{notify: make(chan struct{}, 1)}
}

func (q *ctrlQueue) push(m any) {
	q.mu.Lock()
	q.items = append(q.items, m)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *ctrlQueue) pop() (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, false
	}
	m := q.items[0]
	q.items = q.items[1:]
	return m, true
}

// wait, kuyruğa eleman eklendiğinde sinyal alan kanalı döner. Sinyal
// birleştirilebilir (coalesced); uyandıktan sonra pop ile boşalana kadar
// çekmek çağıranın işidir.
func (q *ctrlQueue) wait() <-chan struct{} { return q.notify }
