package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var secret = []byte("test-secret")

func TestSignVerifyRoundTrip(t *testing.T) {
	want := Claims{Sub: "p-42", Name: "metin", Exp: time.Now().Add(time.Hour).Unix()}
	tok, err := Sign(secret, want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(secret, tok)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("claims = %+v, want %+v", got, want)
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	tok, _ := Sign(secret, Claims{Sub: "p-1", Exp: time.Now().Add(time.Hour).Unix()})
	parts := strings.Split(tok, ".")

	// Payload'ı değiştir (sub'ı yükselt), imza aynı kalsın.
	forged := parts[0] + "." + b64.EncodeToString([]byte(`{"sub":"admin","exp":9999999999}`)) + "." + parts[2]
	if _, err := Verify(secret, forged); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("forged payload = %v, want ErrInvalidToken", err)
	}

	// Header'ı değiştir ("alg":"none" klasiği).
	noneHeader := b64.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	forged = noneHeader + "." + parts[1] + "."
	if _, err := Verify(secret, forged); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("alg:none = %v, want ErrInvalidToken", err)
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	tok, _ := Sign(secret, Claims{Sub: "p-1", Exp: time.Now().Add(time.Hour).Unix()})
	if _, err := Verify([]byte("other-secret"), tok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("wrong secret = %v, want ErrInvalidToken", err)
	}
}

func TestVerifyRejectsExpired(t *testing.T) {
	tok, _ := Sign(secret, Claims{Sub: "p-1", Exp: time.Now().Add(-time.Minute).Unix()})
	if _, err := Verify(secret, tok); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired = %v, want ErrExpired", err)
	}
}

func TestVerifyRejectsGarbage(t *testing.T) {
	for _, tok := range []string{"", "a", "a.b", "a.b.c.d", "!!!.???.###"} {
		if _, err := Verify(secret, tok); err == nil {
			t.Fatalf("garbage %q accepted", tok)
		}
	}
}
