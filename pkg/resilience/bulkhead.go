package resilience

import (
	"context"
	"errors"
	"time"
)

// ErrFull: bölme dolu, iş kabul edilmedi (load shedding).
var ErrFull = errors.New("resilience: bulkhead is full")

// Bulkhead, bir bağımlılığa ayrılan EŞZAMANLILIK bütçesidir. Geminin su
// geçirmez bölmeleri gibi: bir bölme dolarsa (yavaş bağımlılık) gemi
// batmaz, yalnız o bölme etkilenir.
//
// Neden gerekli? Devre kesici zamanda yalıtır ama bağımlılık YAVAŞ
// (hata vermiyor, sadece geç) ise devre açılmaz — çağrılar birikir ve
// tüm goroutine/bağlantı bütçesini yer. Bulkhead bu birikimi tavanlar:
// tavan aşılınca hızlıca ErrFull döner (kuyruğa girmek yerine yükü at).
type Bulkhead struct {
	sem  chan struct{}
	wait time.Duration
}

// NewBulkhead, limit eşzamanlı işe izin veren bir bölme kurar. wait > 0
// ise slot için o kadar beklenir; 0 ise beklemeden reddedilir.
func NewBulkhead(limit int, wait time.Duration) *Bulkhead {
	if limit <= 0 {
		limit = 1
	}
	return &Bulkhead{sem: make(chan struct{}, limit), wait: wait}
}

// Do, fn'i bölme içinde çalıştırır. Slot yoksa (ve bekleme süresi
// dolarsa) ErrFull döner.
func (b *Bulkhead) Do(ctx context.Context, fn func() error) error {
	if err := b.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-b.sem }() // panikte bile slot geri verilir
	return fn()
}

func (b *Bulkhead) acquire(ctx context.Context) error {
	if b.wait <= 0 {
		select {
		case b.sem <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return ErrFull
		}
	}
	timer := time.NewTimer(b.wait)
	defer timer.Stop()
	select {
	case b.sem <- struct{}{}:
		return nil
	case <-timer.C:
		return ErrFull
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InFlight, şu an çalışan iş sayısı (gözlem).
func (b *Bulkhead) InFlight() int { return len(b.sem) }

// Capacity, bölme kapasitesi.
func (b *Bulkhead) Capacity() int { return cap(b.sem) }

// Guard, bulkhead + circuit breaker bileşimidir. Sıra bilinçli:
// BULKHEAD DIŞTA, breaker içte.
//
// Neden? Bulkhead'in ürettiği ErrFull bizim DOYMUŞLUĞUMUZU anlatır,
// bağımlılığın bozukluğunu değil. İçte olsaydı bu hatalar devre
// kesicinin hata sayacını besler ve sırf yük yüzünden devreyi açardı —
// sağlıklı bir bağımlılığı cezalandırmak. Dışta olunca breaker yalnız
// gerçek çağrı sonuçlarını görür. Devre açıkken slot anlık tutulur ve
// hemen bırakılır (ErrOpen anında döner), maliyeti yok sayılır.
type Guard struct {
	Breaker  *Breaker
	Bulkhead *Bulkhead
}

// Do, korumaları sırayla uygular.
func (g Guard) Do(ctx context.Context, fn func() error) error {
	run := fn
	if g.Breaker != nil {
		inner := run
		run = func() error { return g.Breaker.Do(inner) }
	}
	if g.Bulkhead != nil {
		return g.Bulkhead.Do(ctx, run)
	}
	return run()
}
