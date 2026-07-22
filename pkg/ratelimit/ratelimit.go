// Package ratelimit, sıfırdan token bucket hız sınırlayıcıdır.
//
// Rate limiting ile load shedding aynı madalyonun iki yüzüdür:
//   - Hız sınırı ADALET ve kötüye kullanım içindir: tek bir istemci
//     kaynakların tamamını yiyemesin.
//   - Yük atma (load shedding) HAYATTA KALMA içindir: kapasite aşılınca
//     fazlasını hızlıca reddetmek, hepsini kabul edip topluca çökmekten
//     iyidir. "Kısmi hizmet > hiç hizmet."
//
// Token bucket neden? Sabit pencere sayacı pencere sınırında iki kat
// yüke izin verir; kayan pencere pahalıdır. Token bucket hem ortalama
// hızı (rate) hem ANLIK PATLAMAYI (burst) ayrı ayrı ifade eder — oyun
// girdisi gibi düzensiz ama sınırlı akışlar için doğru model.
//
// Jetonlar tembel doldurulur: zamanlayıcı yok, her çağrıda geçen süre
// kadar jeton eklenir (O(1), goroutine'siz).
package ratelimit

import (
	"sync"
	"time"
)

// Bucket, tek bir token bucket'tır. Eşzamanlı kullanıma güvenlidir.
type Bucket struct {
	rate  float64 // saniyede eklenen jeton
	burst float64 // tavan (anlık patlama kapasitesi)
	now   func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// Options, sınırlayıcı ayarları.
type Options struct {
	// Rate, saniyede kaç işlem (ortalama).
	Rate float64
	// Burst, biriktirilebilecek en fazla jeton (anlık patlama).
	Burst int
	// Now, saat kaynağı (testlerde enjekte edilir).
	Now func() time.Time
}

func (o *Options) withDefaults() {
	if o.Rate <= 0 {
		o.Rate = 10
	}
	if o.Burst <= 0 {
		o.Burst = int(o.Rate)
		if o.Burst <= 0 {
			o.Burst = 1
		}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// New, dolu bir kova ile başlar (ilk patlamaya izin verilir).
func New(opts Options) *Bucket {
	opts.withDefaults()
	return &Bucket{
		rate:   opts.Rate,
		burst:  float64(opts.Burst),
		now:    opts.Now,
		tokens: float64(opts.Burst),
		last:   opts.Now(),
	}
}

// Allow, bir jeton harcamayı dener.
func (b *Bucket) Allow() bool { return b.AllowN(1) }

// AllowN, n jeton harcamayı dener; yetmezse hiç harcamaz (ya hep ya hiç).
func (b *Bucket) AllowN(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// Tokens, mevcut jeton sayısı (gözlem/test).
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	return b.tokens
}

func (b *Bucket) refillLocked() {
	now := b.now()
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

// Keyed, anahtar başına (oyuncu/bağlantı/IP) ayrı kova tutar: bir
// istemcinin taşkınlığı diğerlerini etkilemez.
type Keyed struct {
	opts Options

	mu       sync.Mutex
	buckets  map[string]*Bucket
	lastSeen map[string]time.Time
}

func NewKeyed(opts Options) *Keyed {
	opts.withDefaults()
	return &Keyed{
		opts:     opts,
		buckets:  map[string]*Bucket{},
		lastSeen: map[string]time.Time{},
	}
}

// Allow, key için bir jeton harcamayı dener.
func (k *Keyed) Allow(key string) bool {
	k.mu.Lock()
	b, ok := k.buckets[key]
	if !ok {
		b = New(k.opts)
		k.buckets[key] = b
	}
	k.lastSeen[key] = k.opts.Now()
	k.mu.Unlock()
	return b.Allow()
}

// Cleanup, idle süresince görülmemiş kovaları siler (bellek sızıntısını
// önler: her bağlantı/oyuncu için kova birikmesin).
func (k *Keyed) Cleanup(idle time.Duration) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.opts.Now()
	removed := 0
	for key, seen := range k.lastSeen {
		if now.Sub(seen) >= idle {
			delete(k.buckets, key)
			delete(k.lastSeen, key)
			removed++
		}
	}
	return removed
}

// Size, izlenen anahtar sayısı.
func (k *Keyed) Size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.buckets)
}
