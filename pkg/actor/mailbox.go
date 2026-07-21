package actor

import "sync"

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
