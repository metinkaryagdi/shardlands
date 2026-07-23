package auth

import (
	"errors"
	"testing"
	"time"
)

func claims(sub string, ttl time.Duration) Claims {
	return Claims{Sub: sub, Name: sub, Exp: time.Now().Add(ttl).Unix()}
}

// Rotasyonun ASIL İDDİASI: yeni anahtar devreye girdiğinde ESKİ
// anahtarla imzalanmış token'lar geçerliliğini korur. Bu test tutmazsa
// rotasyon "herkesi düşür" demektir.
func TestRotationKeepsOldTokensValid(t *testing.T) {
	eski := []byte("eski-anahtar")
	ring := NewKeyring(eski)

	tokenEski, err := ring.Sign(claims("p-1", time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	// 1. adım: yeni anahtar başa geçer, eski doğrulamada kalır.
	yeni := []byte("yeni-anahtar")
	ring.Set(yeni, eski)

	if _, err := ring.Verify(tokenEski); err != nil {
		t.Fatalf("eski token rotasyondan sonra geçersiz oldu: %v", err)
	}

	// Yeni token'lar YENİ anahtarla imzalanmalı.
	tokenYeni, err := ring.Sign(claims("p-2", time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(yeni, tokenYeni); err != nil {
		t.Fatalf("yeni token yeni anahtarla doğrulanmadı: %v", err)
	}
	if _, err := Verify(eski, tokenYeni); err == nil {
		t.Fatal("yeni token ESKİ anahtarla doğrulandı — imzalama anahtarı dönmemiş")
	}

	// 3. adım: eski anahtar düşürülür → eski token'lar artık geçersiz.
	ring.Set(yeni)
	if _, err := ring.Verify(tokenEski); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("eski anahtar düşürüldükten sonra hata = %v, beklenen ErrInvalidToken", err)
	}
}

func TestKeyringRejectsForeignToken(t *testing.T) {
	ring := NewKeyring([]byte("a"), []byte("b"))
	yabanci, err := Sign([]byte("baska-sistem"), claims("p-9", time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Verify(yabanci); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("hata = %v, beklenen ErrInvalidToken", err)
	}
}

// Süresi dolmuş token için diğer anahtarları denemek anlamsızdır:
// süre imzadan bağımsız bir kontroldür ve hata ErrExpired kalmalıdır
// (istemciye "tekrar giriş yap" demek için gereken ayrım).
func TestKeyringExpiredBeatsOtherKeys(t *testing.T) {
	key := []byte("k")
	ring := NewKeyring(key, []byte("baska"))
	tok, err := Sign(key, Claims{Sub: "p-1", Exp: time.Now().Add(-time.Second).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Verify(tok); !errors.Is(err, ErrExpired) {
		t.Fatalf("hata = %v, beklenen ErrExpired", err)
	}
}

func TestEmptyKeyring(t *testing.T) {
	ring := NewKeyring()
	if _, err := ring.Sign(claims("p-1", time.Hour)); !errors.Is(err, ErrNoKeys) {
		t.Fatalf("Sign hatası = %v, beklenen ErrNoKeys", err)
	}
	if _, err := ring.Verify("a.b.c"); !errors.Is(err, ErrNoKeys) {
		t.Fatalf("Verify hatası = %v, beklenen ErrNoKeys", err)
	}
}

// Sıcak yükleme okuma yolunu bloklamamalı ve yarım liste
// göstermemeli: -race altında koşar.
func TestKeyringConcurrentRotation(t *testing.T) {
	a, b := []byte("a"), []byte("b")
	ring := NewKeyring(a)
	tok, err := ring.Sign(claims("p-1", time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			ring.Set(b, a)
			ring.Set(a, b)
		}
	}()
	for i := 0; i < 500; i++ {
		if _, err := ring.Verify(tok); err != nil {
			t.Errorf("rotasyon sırasında doğrulama düştü: %v", err)
			break
		}
	}
	<-done
}
