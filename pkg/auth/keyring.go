package auth

import (
	"errors"
	"sync/atomic"
)

// Keyring, İMZALAMA ANAHTARI ROTASYONUNU mümkün kılan yapıdır.
//
// PROBLEM. Tek anahtarla rotasyon imkânsızdır: anahtarı değiştirdiğin
// anda daha önce basılmış BÜTÜN token'lar geçersiz olur ve o an oyunda
// olan herkes düşer. Token'ın süresi 24 saat olduğu için "eskiler
// dolsun, sonra değiştiririz" demek de rotasyonu 24 saate yaymak
// demektir — ihlal durumunda kimsenin bekleyecek vakti yoktur.
//
// ÇÖZÜM. İmzalarken TEK anahtar (en yenisi), doğrularken ÇOKLU anahtar
// (yeni + eski). Rotasyon üç adıma iner:
//
//  1. Yeni anahtar listenin başına eklenir. Yeni token'lar onunla
//     imzalanır; eski token'lar hâlâ ikinci anahtarla doğrulanır.
//  2. Eski token'ların süresi dolana kadar beklenir.
//  3. Eski anahtar listeden düşürülür.
//
// Bu, "Vault kullanıyoruz" diyen kurulumların çoğunda sessizce eksik
// olan parçadır: sır depolanır ama DEĞİŞTİRİLEMEZ, çünkü uygulama tek
// anahtar varsayar.
//
// NEDEN `kid` YOK? JWT header'ına anahtar kimliği koyup doğrudan doğru
// anahtarı seçebilirdik. Yapmadık: bu paketin güvenlik dayanağı
// header'ın SABİT olması ve token'dan hiçbir şey okumamamız
// (bkz. jwt.go — "alg:none" panzehiri). Birkaç anahtarı sırayla denemek
// birkaç HMAC hesabına mal olur; header'ı yorumlamaya başlamak ise
// saldırı yüzeyi açar. Anahtar sayısı büyürse (JWKS ölçeği) bu takas
// tersine döner.
type Keyring struct {
	// keys[0] imzalama anahtarıdır; tamamı doğrulamada denenir.
	// atomic.Pointer: sıcak yükleme sırasında istekler kilitlenmesin
	// (okuma yolu çok sık, yazma yolu dakikada bir).
	keys atomic.Pointer[[][]byte]
}

var ErrNoKeys = errors.New("auth: keyring is empty")

// NewKeyring, verilen anahtarlarla bir zincir kurar. İlk anahtar
// imzalama anahtarıdır.
func NewKeyring(keys ...[]byte) *Keyring {
	k := &Keyring{}
	k.Set(keys...)
	return k
}

// Set, anahtar listesini ATOMİK olarak değiştirir. Akıştaki istekler
// ya eski ya yeni listeyi görür; yarım liste görmez.
func (k *Keyring) Set(keys ...[]byte) {
	cp := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) > 0 {
			cp = append(cp, key)
		}
	}
	k.keys.Store(&cp)
}

// Keys, geçerli anahtar listesinin kopyasını döner (gözlem/test).
func (k *Keyring) Keys() [][]byte {
	p := k.keys.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Sign, EN YENİ anahtarla imzalar.
func (k *Keyring) Sign(c Claims) (string, error) {
	keys := k.Keys()
	if len(keys) == 0 {
		return "", ErrNoKeys
	}
	return Sign(keys[0], c)
}

// Verify, anahtarları sırayla dener. Süresi dolmuş token için hemen
// ErrExpired döner — başka anahtar denemek anlamsızdır, çünkü süre
// imzadan bağımsızdır.
func (k *Keyring) Verify(token string) (Claims, error) {
	keys := k.Keys()
	if len(keys) == 0 {
		return Claims{}, ErrNoKeys
	}
	for _, key := range keys {
		c, err := Verify(key, token)
		switch {
		case err == nil:
			return c, nil
		case errors.Is(err, ErrExpired):
			return Claims{}, err
		}
	}
	return Claims{}, ErrInvalidToken
}
