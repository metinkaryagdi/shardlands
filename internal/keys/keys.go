// Package keys, JWT imzalama anahtarlarının NEREDEN geldiğini tek bir
// yerde toplar. cmd/server ve cmd/player aynı kararı iki kez vermesin
// diye ayrı paket.
//
// Öncelik sırası ve gerekçesi:
//
//  1. VAULT_ADDR varsa Vault (kümedeki üretim yolu).
//  2. Yoksa SHARDLANDS_SECRET (tek süreç geliştirme).
//  3. O da yoksa sabit geliştirme sırrı + uyarı.
//
// Sıranın açık olması önemli: "sır nereden geldi" sorusunun cevabı
// çalışma anında log'a yazılıyor. Bir kurulumun yanlışlıkla geliştirme
// sırrıyla üretime çıkması, sessizce olabilecek en pahalı hatalardan
// biri — bu yüzden 3. seçenek gürültülü.
package keys

import (
	"context"
	"log"
	"os"
	"time"

	"shardlands/pkg/auth"
	"shardlands/pkg/vault"
)

const devSecret = "dev-secret-change-me"

// Load, anahtar zincirini kurar. Dönen fonksiyon arka plan tazelemesini
// durdurur (Vault kullanılmıyorsa no-op).
func Load(ctx context.Context) (*auth.Keyring, func(), error) {
	addr := os.Getenv("VAULT_ADDR")
	if addr == "" {
		secret := os.Getenv("SHARDLANDS_SECRET")
		if secret == "" {
			secret = devSecret
			log.Println("uyarı: VAULT_ADDR ve SHARDLANDS_SECRET yok, GELİŞTİRME SIRRI kullanılıyor")
		} else {
			log.Println("anahtar kaynağı: SHARDLANDS_SECRET (ortam değişkeni)")
		}
		return auth.NewKeyring([]byte(secret)), func() {}, nil
	}

	c, err := vault.New(vault.Options{
		Addr: addr,
		Role: envOr("VAULT_ROLE", "shardlands"),
	})
	if err != nil {
		return nil, nil, err
	}
	src := &vault.KeySource{
		Client:   c,
		Mount:    envOr("VAULT_KV_MOUNT", "secret"),
		Path:     envOr("VAULT_KV_PATH", "shardlands/jwt"),
		Interval: envDuration("VAULT_REFRESH", 30*time.Second),
		Keyring:  auth.NewKeyring(),
	}
	// İlk okuma BAŞARISIZ OLURSA süreç başlamaz. Burada geliştirme
	// sırrına düşmek, "Vault kurulu sanıp aslında sabit sırla koşmak"
	// demek olurdu — sessiz ve tehlikeli.
	if err := src.Load(ctx); err != nil {
		return nil, nil, err
	}
	log.Printf("anahtar kaynağı: Vault (%s, %s/%s)", addr, src.Mount, src.Path)
	return src.Keyring, src.Start(ctx), nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("uyarı: %s ayrıştırılamadı (%q), varsayılan %s", k, v, def)
	}
	return def
}
