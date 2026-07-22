package outbox

import (
	"context"
	"fmt"
	"sync"

	"shardlands/pkg/bus"
	"shardlands/pkg/es"
)

// Consume, bus'tan event akışı tüketen read model'ler için standart
// yardımcıdır:
//
//   - Envelope'u çözer ve es.Event'e döndürür.
//   - GLOBAL SIRA İLE DEDUPE eder: at-least-once teslimde aynı event
//     birden çok kez gelebilir (nak/yeniden teslim, relay restart'ı);
//     read model'in iki kez uygulanması bakiyeleri/sayaçları bozardı.
//     Idempotentliğin doğru yeri budur — teslim katmanı değil.
//   - GEÇİCİ (ephemeral) tüketici kullanır: süreç her başladığında akış
//     baştan oynatılır ve in-memory model sıfırdan kurulur. Kalıcı
//     tüketici olsaydı yalnız yeni event'ler gelir, geçmiş kaybolurdu.
//   - MaxInFlight=1: teslim SIRALI olur. Read model'ler sıraya duyarlı
//     (önce Reserved sonra Released); paralellik yerine doğruluk.
//
// apply hata dönerse mesaj nak'lanır ve yeniden teslim edilir; denemeler
// tükenirse mesaj DLQ'ya taşınır (akış tıkanmaz).
func Consume(b bus.Bus, name string, apply func(es.Event) error) (bus.Subscription, error) {
	if name == "" {
		return nil, fmt.Errorf("outbox: consumer name is required")
	}
	var (
		mu          sync.Mutex
		lastApplied uint64
	)
	return b.Subscribe(bus.SubscribeOptions{
		Name:        name,
		Durable:     false, // her başlangıçta baştan oynat
		MaxInFlight: 1,     // sıralı
	}, func(_ context.Context, m bus.Message) error {
		env, err := Decode(m.Data)
		if err != nil {
			// Çözülemeyen mesaj kalıcı hatadır: yeniden denemek işe
			// yaramaz, DLQ'ya gitmesi doğru.
			return fmt.Errorf("outbox: decode: %w", err)
		}
		mu.Lock()
		if env.Global <= lastApplied {
			mu.Unlock()
			return nil // zaten uygulanmış (dedupe)
		}
		mu.Unlock()

		if err := apply(env.ToEvent()); err != nil {
			return err
		}

		mu.Lock()
		if env.Global > lastApplied {
			lastApplied = env.Global
		}
		mu.Unlock()
		return nil
	})
}
