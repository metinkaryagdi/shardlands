package storage

import "encoding/binary"

// Bloom filter: "bu anahtar bu SSTable'da KESİNLİKLE YOK" diyebilen
// olasılıksal küme. Yanlış negatif imkânsız, yanlış pozitif nadir
// (10 bit/anahtar için ~%1). LSM okuma yolunda kritik: bir Get,
// anahtarı içermeyen her tablonun diskine inmeden bloom'dan döner.
//
// k hash yerine tek 64-bit FNV-1a hash'inin iki yarısıyla "double
// hashing" kullanılır: g_i = h1 + i*h2 (Kirsch-Mitzenmacher). Pratik
// yanlış pozitif oranı k bağımsız hash ile aynı kalır.
type bloom struct {
	k    uint8
	bits []byte
}

func bloomHash(b []byte) uint64 {
	h := uint64(14695981039346656037) // FNV-1a offset basis
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211 // FNV prime
	}
	return h
}

// newBloom, toplanmış anahtar hash'lerinden filtre kurar. bitsPerKey
// arttıkça yanlış pozitif düşer, dosya büyür (10 = LevelDB varsayılanı).
func newBloom(hashes []uint64, bitsPerKey int) *bloom {
	k := int(float64(bitsPerKey) * 0.69) // ln2 * bitsPerKey ≈ optimal k
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	nbits := len(hashes) * bitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	nbits = (nbits + 7) &^ 7 // bayta yuvarla
	b := &bloom{k: uint8(k), bits: make([]byte, nbits/8)}
	for _, h := range hashes {
		h1, h2 := uint32(h), uint32(h>>32)|1 // h2 tek sayı: tüm bitleri gezer
		for i := 0; i < k; i++ {
			bit := (h1 + uint32(i)*h2) % uint32(nbits)
			b.bits[bit/8] |= 1 << (bit % 8)
		}
	}
	return b
}

func (b *bloom) mayContain(h uint64) bool {
	nbits := uint32(len(b.bits) * 8)
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := 0; i < int(b.k); i++ {
		bit := (h1 + uint32(i)*h2) % nbits
		if b.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloom) marshal(dst []byte) []byte {
	dst = append(dst, b.k)
	dst = binary.AppendUvarint(dst, uint64(len(b.bits)))
	return append(dst, b.bits...)
}

func unmarshalBloom(buf []byte) (*bloom, error) {
	if len(buf) < 2 {
		return nil, ErrCorrupt
	}
	k := buf[0]
	nbytes, n := binary.Uvarint(buf[1:])
	if n <= 0 || uint64(len(buf[1+n:])) != nbytes {
		return nil, ErrCorrupt
	}
	return &bloom{k: k, bits: buf[1+n:]}, nil
}
