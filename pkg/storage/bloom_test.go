package storage

import (
	"fmt"
	"testing"
)

// Bloom: yanlış negatif ASLA olmamalı; yanlış pozitif oranı 10
// bit/anahtar için makul (%~1, testte gevşek %5 sınırı) kalmalı.
func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 10000
	hashes := make([]uint64, n)
	for i := range hashes {
		hashes[i] = bloomHash([]byte(fmt.Sprintf("member-%d", i)))
	}
	b := newBloom(hashes, 10)

	for i := 0; i < n; i++ {
		if !b.mayContain(bloomHash([]byte(fmt.Sprintf("member-%d", i)))) {
			t.Fatalf("false negative for member-%d (must be impossible)", i)
		}
	}

	falsePos := 0
	for i := 0; i < n; i++ {
		if b.mayContain(bloomHash([]byte(fmt.Sprintf("stranger-%d", i)))) {
			falsePos++
		}
	}
	if rate := float64(falsePos) / n; rate > 0.05 {
		t.Fatalf("false positive rate %.4f, want <= 0.05", rate)
	}
}

func TestBloomRoundTrip(t *testing.T) {
	hashes := []uint64{bloomHash([]byte("a")), bloomHash([]byte("b"))}
	b := newBloom(hashes, 10)
	buf := b.marshal(nil)
	got, err := unmarshalBloom(buf)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.k != b.k || len(got.bits) != len(b.bits) {
		t.Fatalf("round trip mismatch: k=%d/%d bits=%d/%d", got.k, b.k, len(got.bits), len(b.bits))
	}
	if !got.mayContain(bloomHash([]byte("a"))) {
		t.Fatal("member lost in round trip")
	}
	if _, err := unmarshalBloom([]byte{7}); err == nil {
		t.Fatal("truncated bloom must fail to unmarshal")
	}
}
