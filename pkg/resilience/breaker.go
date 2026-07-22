// Package resilience, sıfırdan yazılmış dayanıklılık kalıplarıdır:
// circuit breaker (devre kesici) ve bulkhead (bölme).
//
// İkisi de AYNI hastalığa karşıdır: bir alt bağımlılığın yavaşlaması
// veya bozulması, çağıranı da düşürür (kaskad arıza). Zincir şöyle
// kurulur: yavaş bağımlılık → çağrılar birikir → goroutine/bağlantı
// tükenir → çağıran servis de cevap veremez → onun çağıranı da...
//
//   - Circuit breaker ZAMANDA yalıtır: bağımlılık bozukken çağırmayı
//     bırakır (hızlı başarısızlık), ona toparlanma alanı verir.
//   - Bulkhead EŞZAMANLILIKTA yalıtır: bir bağımlılığa ayrılan bütçe
//     tükenirse fazlası reddedilir, diğer işler etkilenmez.
package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen: devre açık, çağrı denenmeden reddedildi.
var ErrOpen = errors.New("resilience: circuit breaker is open")

// State, devre kesicinin durumu.
type State int

const (
	// Closed: normal çalışma; hatalar sayılır.
	Closed State = iota
	// Open: bağımlılık bozuk kabul edilir; çağrılar DENENMEDEN reddedilir
	// (hızlı başarısızlık). Bu, hem çağıranı timeout birikiminden korur
	// hem bağımlılığa nefes aldırır.
	Open
	// HalfOpen: cooldown doldu; SINIRLI sayıda deneme geçirilir. Başarı
	// gelirse kapanır, hata gelirse yeniden açılır. "Bir yudum su" testi.
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// BreakerOptions, devre kesici ayarları.
type BreakerOptions struct {
	// FailureThreshold, Closed durumunda arka arkaya kaç hatadan sonra
	// devrenin açılacağı (varsayılan 5).
	FailureThreshold int
	// OpenDuration, Open'da ne kadar bekleyip HalfOpen'a geçileceği
	// (varsayılan 5s).
	OpenDuration time.Duration
	// HalfOpenMax, HalfOpen'da aynı anda kaç denemeye izin verildiği
	// (varsayılan 1). Küçük tutulur: bozuk olabilecek bağımlılığa
	// yüklenmemek için.
	HalfOpenMax int
	// HalfOpenSuccesses, kapanmak için gereken ardışık başarı
	// (varsayılan 1).
	HalfOpenSuccesses int
	// Now, saat kaynağı (testlerde enjekte edilir).
	Now func() time.Time
}

func (o *BreakerOptions) withDefaults() {
	if o.FailureThreshold <= 0 {
		o.FailureThreshold = 5
	}
	if o.OpenDuration <= 0 {
		o.OpenDuration = 5 * time.Second
	}
	if o.HalfOpenMax <= 0 {
		o.HalfOpenMax = 1
	}
	if o.HalfOpenSuccesses <= 0 {
		o.HalfOpenSuccesses = 1
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Breaker, devre kesicidir. Eşzamanlı kullanıma güvenlidir.
type Breaker struct {
	opts BreakerOptions

	mu           sync.Mutex
	state        State
	failures     int
	openedAt     time.Time
	halfOpenBusy int
	halfOpenOK   int
}

func NewBreaker(opts BreakerOptions) *Breaker {
	opts.withDefaults()
	return &Breaker{opts: opts}
}

// State, güncel durumu döner (gerekirse Open→HalfOpen geçişini yapar).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpenLocked()
	return b.state
}

// maybeHalfOpenLocked: Open'da cooldown dolduysa HalfOpen'a geç.
func (b *Breaker) maybeHalfOpenLocked() {
	if b.state == Open && b.opts.Now().Sub(b.openedAt) >= b.opts.OpenDuration {
		b.state = HalfOpen
		b.halfOpenBusy = 0
		b.halfOpenOK = 0
	}
}

// Do, fn'i devre kurallarına göre çalıştırır. Devre açıksa fn HİÇ
// çağrılmaz ve ErrOpen döner.
func (b *Breaker) Do(fn func() error) error {
	if err := b.acquire(); err != nil {
		return err
	}
	err := fn()
	b.report(err)
	return err
}

// acquire, çağrının geçip geçemeyeceğine karar verir.
func (b *Breaker) acquire() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpenLocked()

	switch b.state {
	case Open:
		return ErrOpen
	case HalfOpen:
		if b.halfOpenBusy >= b.opts.HalfOpenMax {
			return ErrOpen // deneme kotası dolu
		}
		b.halfOpenBusy++
		return nil
	default: // Closed
		return nil
	}
}

// report, sonucu işler ve durum geçişlerini yapar.
func (b *Breaker) report(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case HalfOpen:
		b.halfOpenBusy--
		if err != nil {
			// Deneme başarısız: yeniden aç ve cooldown'ı sıfırla.
			b.trip()
			return
		}
		b.halfOpenOK++
		if b.halfOpenOK >= b.opts.HalfOpenSuccesses {
			b.state = Closed
			b.failures = 0
			b.halfOpenOK = 0
		}
	case Closed:
		if err != nil {
			b.failures++
			if b.failures >= b.opts.FailureThreshold {
				b.trip()
			}
			return
		}
		b.failures = 0 // başarı seriyi kırar
	}
}

func (b *Breaker) trip() {
	b.state = Open
	b.openedAt = b.opts.Now()
	b.failures = 0
	b.halfOpenBusy = 0
	b.halfOpenOK = 0
}
