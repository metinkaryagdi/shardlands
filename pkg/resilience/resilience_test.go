package resilience

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// fakeClock, deterministik zaman (devre kesici cooldown testleri için).
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

// Eşik dolunca devre açılır ve çağrılar DENENMEDEN reddedilir.
func TestBreakerOpensAndFailsFast(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := NewBreaker(BreakerOptions{FailureThreshold: 3, OpenDuration: time.Minute, Now: clk.Now})

	var calls atomic.Int32
	fail := func() error { calls.Add(1); return errBoom }

	for i := 0; i < 3; i++ {
		if err := b.Do(fail); !errors.Is(err, errBoom) {
			t.Fatalf("call %d = %v, want errBoom", i, err)
		}
	}
	if b.State() != Open {
		t.Fatalf("state = %s, want open", b.State())
	}

	before := calls.Load()
	if err := b.Do(fail); !errors.Is(err, ErrOpen) {
		t.Fatalf("open circuit = %v, want ErrOpen", err)
	}
	if calls.Load() != before {
		t.Fatal("function was called while circuit is open (must fail fast)")
	}
}

// Closed'da başarı, hata serisini kırar (devre açılmaz).
func TestBreakerSuccessResetsFailures(t *testing.T) {
	b := NewBreaker(BreakerOptions{FailureThreshold: 3})
	b.Do(func() error { return errBoom })
	b.Do(func() error { return errBoom })
	b.Do(func() error { return nil }) // seri kırıldı
	b.Do(func() error { return errBoom })
	b.Do(func() error { return errBoom })
	if b.State() != Closed {
		t.Fatalf("state = %s, want closed (success reset the streak)", b.State())
	}
}

// Cooldown dolunca HalfOpen; başarı gelirse kapanır.
func TestBreakerHalfOpenRecovers(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := NewBreaker(BreakerOptions{FailureThreshold: 1, OpenDuration: 10 * time.Second, Now: clk.Now})

	b.Do(func() error { return errBoom })
	if b.State() != Open {
		t.Fatalf("state = %s, want open", b.State())
	}
	clk.Advance(11 * time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("state = %s, want half-open after cooldown", b.State())
	}
	if err := b.Do(func() error { return nil }); err != nil {
		t.Fatalf("half-open trial = %v", err)
	}
	if b.State() != Closed {
		t.Fatalf("state = %s, want closed after successful trial", b.State())
	}
}

// HalfOpen'da hata → yeniden Open (ve cooldown sıfırlanır).
func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := NewBreaker(BreakerOptions{FailureThreshold: 1, OpenDuration: 10 * time.Second, Now: clk.Now})

	b.Do(func() error { return errBoom })
	clk.Advance(11 * time.Second)
	if b.State() != HalfOpen {
		t.Fatal("expected half-open")
	}
	b.Do(func() error { return errBoom }) // deneme başarısız
	if b.State() != Open {
		t.Fatalf("state = %s, want open again", b.State())
	}
	// Cooldown yeniden başladı: hemen HalfOpen olmamalı.
	clk.Advance(5 * time.Second)
	if b.State() != Open {
		t.Fatalf("state = %s, want still open (cooldown restarted)", b.State())
	}
}

// HalfOpen'da eşzamanlı deneme sayısı sınırlıdır.
func TestBreakerHalfOpenLimitsTrials(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	b := NewBreaker(BreakerOptions{
		FailureThreshold: 1, OpenDuration: time.Second, HalfOpenMax: 1, Now: clk.Now,
	})
	b.Do(func() error { return errBoom })
	clk.Advance(2 * time.Second)

	started := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Do(func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	// İlk deneme sürerken ikincisi kotayı aşar.
	if err := b.Do(func() error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("second half-open trial = %v, want ErrOpen (quota)", err)
	}
	close(release)
	wg.Wait()
}

// Bulkhead: kapasite kadar eşzamanlı iş; fazlası ErrFull.
func TestBulkheadLimitsConcurrency(t *testing.T) {
	bh := NewBulkhead(2, 0)
	ctx := context.Background()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bh.Do(ctx, func() error {
				started <- struct{}{}
				<-release
				return nil
			})
		}()
	}
	<-started
	<-started

	if err := bh.Do(ctx, func() error { return nil }); !errors.Is(err, ErrFull) {
		t.Fatalf("third call = %v, want ErrFull", err)
	}
	if got := bh.InFlight(); got != 2 {
		t.Fatalf("in-flight = %d, want 2", got)
	}

	close(release)
	wg.Wait()
	// Slotlar geri verilmeli.
	if err := bh.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("after release = %v, want success", err)
	}
}

// Bekleme süresi verilirse slot boşalınca geçer.
func TestBulkheadWaitsForSlot(t *testing.T) {
	bh := NewBulkhead(1, 2*time.Second)
	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{})

	go func() {
		bh.Do(ctx, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	done := make(chan error, 1)
	go func() { done <- bh.Do(ctx, func() error { return nil }) }()

	time.Sleep(100 * time.Millisecond)
	close(release) // slot boşalt
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiting call = %v, want success", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting call never completed")
	}
}

// Panikte bile slot geri verilir.
func TestBulkheadReleasesOnPanic(t *testing.T) {
	bh := NewBulkhead(1, 0)
	func() {
		defer func() { recover() }()
		bh.Do(context.Background(), func() error { panic("kaza") })
	}()
	if got := bh.InFlight(); got != 0 {
		t.Fatalf("in-flight after panic = %d, want 0", got)
	}
}

// Guard: bulkhead DIŞTA olduğu için ErrFull devreyi AÇMAMALI (kendi
// doymuşluğumuz bağımlılığın suçu değil).
func TestGuardFullDoesNotTripBreaker(t *testing.T) {
	g := Guard{
		Breaker:  NewBreaker(BreakerOptions{FailureThreshold: 2}),
		Bulkhead: NewBulkhead(1, 0),
	}
	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		g.Do(ctx, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	for i := 0; i < 5; i++ {
		if err := g.Do(ctx, func() error { return nil }); !errors.Is(err, ErrFull) {
			t.Fatalf("call %d = %v, want ErrFull", i, err)
		}
	}
	if st := g.Breaker.State(); st != Closed {
		t.Fatalf("breaker = %s, want closed (shedding must not trip it)", st)
	}
	close(release)
}

// Guard: gerçek bağımlılık hataları devreyi açar ve sonrası hızlı
// başarısızlık olur.
func TestGuardBreakerStillTripsOnRealFailures(t *testing.T) {
	g := Guard{
		Breaker:  NewBreaker(BreakerOptions{FailureThreshold: 2, OpenDuration: time.Minute}),
		Bulkhead: NewBulkhead(4, 0),
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		g.Do(ctx, func() error { return errBoom })
	}
	if err := g.Do(ctx, func() error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("after failures = %v, want ErrOpen", err)
	}
}
