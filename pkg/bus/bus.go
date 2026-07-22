// Package bus, event bus soyutlamasıdır: dayanıklı yayın (publish),
// dayanıklı tüketici (durable consumer), açık ack/nak ve yeniden teslim.
//
// Neden bus? Faz 3'e kadar read model'ler event store'a DOĞRUDAN
// aboneydi — aynı süreçte olmak zorundaydılar. Bus, üreticiyi
// tüketiciden ayırır: aynı event akışını farklı süreçler/servisler
// bağımsız hızda tüketebilir (Faz 6'da dağıtmanın ön koşulu).
//
// Teslim garantisi AT-LEAST-ONCE'tır ve bu bilinçlidir: exactly-once
// dağıtık sistemlerde teslim katmanında YOKTUR; doğru yer tüketicinin
// idempotent olmasıdır (bkz. pkg/es global sıra ile dedupe). Yayın
// tarafında JetStream'in Msg-Id dedupe penceresi tekrarları azaltır ama
// garanti etmez.
//
// Hata yolu: handler hata dönerse mesaj NAK'lanır ve geri çekilmeli
// (backoff) yeniden teslim edilir. MaxDeliver denemesi de tükenirse
// mesaj DLQ konusuna taşınır ve ack'lenir — "zehirli mesaj" akışı
// sonsuza dek tıkamaz (poison message sorunu).
package bus

import (
	"context"
	"time"
)

// Message, tüketiciye ulaşan mesaj.
type Message struct {
	Subject string
	Data    []byte
	// ID, yayıncının verdiği idempotency anahtarı (bizde event'in global
	// sırası). Tüketici dedupe için bunu kullanır.
	ID string
	// Deliveries, bu mesajın kaçıncı teslim denemesi olduğu (1 tabanlı).
	Deliveries int
}

// Handler, mesajı işler. nil dönerse ack; hata dönerse nak + yeniden
// teslim (MaxDeliver'a kadar), sonra DLQ.
type Handler func(ctx context.Context, m Message) error

// SubscribeOptions, tüketici ayarları.
type SubscribeOptions struct {
	// Name, tüketicinin adı (DLQ konusu ve kalıcı tüketici kimliği).
	Name string
	// Durable:
	//   true  → kalıcı tüketici: kaldığı yerden devam eder (checkpoint
	//           bus'ta). Durumu KENDİ persist eden tüketiciler için.
	//   false → geçici (ephemeral) tüketici: her başlangıçta akışı
	//           BAŞTAN oynatır. IN-MEMORY read model'ler bunu ister —
	//           süreç yeniden başlayınca model sıfırdan kurulmalı;
	//           kalıcı tüketici olsaydı yalnız yeni event'ler gelir ve
	//           geçmiş kaybolurdu.
	Durable bool
	// Filter, dinlenecek konu deseni (boşsa stream'in tamamı).
	Filter string
	// MaxDeliver, bir mesaj için toplam teslim denemesi (varsayılan 5).
	// Aşılırsa mesaj DLQ'ya taşınır.
	MaxDeliver int
	// AckWait, ack gelmezse yeniden teslim süresi (varsayılan 5s).
	AckWait time.Duration
	// MaxInFlight, aynı anda işlenmemiş (ack bekleyen) mesaj tavanı —
	// BACKPRESSURE budur: tüketici yavaşsa bus daha fazla göndermez.
	MaxInFlight int
	// Backoff, i. denemeden sonra beklenecek süreyi verir (nil ise
	// üstel: 100ms, 200ms, 400ms... 5s tavanlı).
	Backoff func(attempt int) time.Duration
}

func (o *SubscribeOptions) withDefaults() {
	if o.MaxDeliver <= 0 {
		o.MaxDeliver = 5
	}
	if o.AckWait <= 0 {
		o.AckWait = 5 * time.Second
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = 64
	}
	if o.Backoff == nil {
		o.Backoff = ExponentialBackoff(100*time.Millisecond, 5*time.Second)
	}
}

// ExponentialBackoff, üstel geri çekilme üretir (tavanlı).
func ExponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		d := base
		for i := 1; i < attempt && d < max; i++ {
			d *= 2
		}
		if d > max {
			d = max
		}
		return d
	}
}

// Subscription, çalışan bir tüketicidir.
type Subscription interface {
	Stop()
}

// Bus, yayın ve dayanıklı tüketim arayüzü.
type Bus interface {
	// Publish, subject'e yayınlar. id boş değilse yayın tarafı dedupe
	// anahtarı olarak kullanılır.
	Publish(ctx context.Context, subject, id string, data []byte) error
	// Subscribe, dayanıklı bir tüketici başlatır.
	Subscribe(opts SubscribeOptions, h Handler) (Subscription, error)
	// DeadLetters, DLQ'ya düşmüş mesajları tüketmek için (gözlem/onarım).
	DeadLetters(durable string, h Handler) (Subscription, error)
	Close() error
}
