// Package auth, HS256 imzalı minimal bir JWT implementasyonudur.
//
// Neden elle? Bir JWT'nin içinde ne olduğunu bilmek için: üç base64url
// parça (header.payload.signature), imza = HMAC-SHA256(header.payload).
// JWT'nin değeri sunucunun DURUM TUTMAMASIDIR: token'ı doğrulamak için
// veritabanına gitmek gerekmez, imza yeter — gateway her WS bağlantısında
// bunu kullanır. Bedeli: iptal edilemezlik (exp süresine kadar geçerli);
// çözümü (kısa exp + refresh, deny-list) Faz 6'nın konusu.
//
// Üretim notları: alg her zaman sunucuda SABİTTİR (header'dan okunmaz —
// "alg:none" ve RS256→HS256 karıştırma saldırılarının panzehiri);
// karşılaştırma sabit zamanlıdır (hmac.Equal). Çoklu servis/rotasyon
// gerekince RS256/JWKS'e geçilir (Faz 6).
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrInvalidToken = errors.New("auth: invalid token")
	ErrExpired      = errors.New("auth: token expired")
)

// Claims: Faz 1 için gereken asgari alanlar (RFC 7519 kayıtlı isimler).
type Claims struct {
	Sub  string `json:"sub"`  // player id
	Name string `json:"name"` // görünen isim
	Exp  int64  `json:"exp"`  // unix saniye
}

// b64 = padding'siz base64url (RFC 7515'in emrettiği biçim).
var b64 = base64.RawURLEncoding

// encodedHeader sabittir: alg müzakere edilmez.
var encodedHeader = b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// Sign, claims'i imzalı token'a çevirir.
func Sign(secret []byte, c Claims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	signingInput := encodedHeader + "." + b64.EncodeToString(payload)
	return signingInput + "." + b64.EncodeToString(sign(secret, signingInput)), nil
}

// Verify, imzayı ve süreyi doğrulayıp claims'i döner.
func Verify(secret []byte, token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, ErrInvalidToken
	}
	// Header'ı OKUMUYORUZ: beklediğimiz sabit header değilse token bizden
	// çıkmamıştır. (alg'ı token'dan okumak, saldırganın algoritmayı
	// seçmesine izin vermek demektir.)
	if parts[0] != encodedHeader {
		return Claims{}, ErrInvalidToken
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	if !hmac.Equal(sig, sign(secret, parts[0]+"."+parts[1])) {
		return Claims{}, ErrInvalidToken
	}
	payload, err := b64.DecodeString(parts[1])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, ErrInvalidToken
	}
	if c.Exp > 0 && time.Now().Unix() >= c.Exp {
		return Claims{}, ErrExpired
	}
	return c, nil
}

func sign(secret []byte, input string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(input))
	return mac.Sum(nil)
}
