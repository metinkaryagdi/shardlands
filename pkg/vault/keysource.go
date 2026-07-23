package vault

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"shardlands/pkg/auth"
)

// KeySource, Vault'taki bir KV sırrını auth.Keyring'e bağlar ve
// periyodik olarak tazeler.
//
// SIR DEPOLAMAK KOLAY, DEĞİŞTİRMEK ZOR. Vault'un asıl kazancı sırrın
// nerede durduğu değil, DÖNDÜRÜLEBİLİR olmasıdır — ama bu ancak
// uygulama değişikliği fark edip yeni anahtarı alabiliyorsa geçerlidir.
// Sırrı bir kez okuyup değişkende tutan bir uygulama için Vault, düz
// metin Secret'tan daha güvenli ama aynı derecede DONMUŞ bir depodur.
//
// Beklenen KV biçimi:
//
//	jwt_signing_key    -> imzalama anahtarı (zorunlu)
//	jwt_previous_keys  -> virgülle ayrılmış eski anahtarlar (isteğe bağlı)
//
// Rotasyon operatörün üç adımı olur (docs/secrets.md):
//  1. jwt_previous_keys'e eskiyi ekle, jwt_signing_key'i yenile
//  2. token ömrü kadar bekle
//  3. jwt_previous_keys'i boşalt
type KeySource struct {
	Client   *Client
	Mount    string // örn "secret"
	Path     string // örn "shardlands/jwt"
	Interval time.Duration
	Keyring  *auth.Keyring
}

const (
	fieldSigning  = "jwt_signing_key"
	fieldPrevious = "jwt_previous_keys"
)

// Load, sırrı bir kez okur ve keyring'i günceller.
func (s *KeySource) Load(ctx context.Context) error {
	fields, err := s.Client.ReadKV(ctx, s.Mount, s.Path)
	if err != nil {
		return err
	}
	signing := strings.TrimSpace(fields[fieldSigning])
	if signing == "" {
		return fmt.Errorf("vault: %s/%s içinde %s yok", s.Mount, s.Path, fieldSigning)
	}
	keys := [][]byte{[]byte(signing)}
	for _, p := range strings.Split(fields[fieldPrevious], ",") {
		if p = strings.TrimSpace(p); p != "" && p != signing {
			keys = append(keys, []byte(p))
		}
	}
	s.Keyring.Set(keys...)
	return nil
}

// Start, arka planda periyodik tazeleme başlatır. Dönen fonksiyon
// tazelemeyi durdurur.
//
// Tazeleme hatası ÖLÜMCÜL DEĞİLDİR: elimizdeki anahtarlarla çalışmaya
// devam ederiz. Vault'un geçici arızası yüzünden bütün girişleri
// reddetmek, sırrı korumak adına servisi düşürmek olurdu — Faz 4'teki
// "bağımlılık arızasında kendini öldürme" ilkesinin aynısı.
func (s *KeySource) Start(ctx context.Context) func() {
	interval := s.Interval
	if interval <= 0 {
		interval = time.Minute
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.Load(ctx); err != nil && ctx.Err() == nil {
					log.Printf("vault: anahtar tazeleme başarısız (eskisiyle devam): %v", err)
				}
			}
		}
	}()
	return cancel
}
