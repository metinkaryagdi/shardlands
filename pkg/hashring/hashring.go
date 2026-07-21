// Package hashring, sıfırdan yazılmış consistent hashing sağlar:
// anahtarları (ör. oyuncu/bölge id'leri) düğümlere (shard'lara) minimal
// yeniden eşlemeyle dağıtır.
//
// Neden naif hash(key) % N değil? N değişince (shard ekle/çıkar) modülo
// neredeyse TÜM anahtarları taşır — stateful sistemde her oyuncunun göçü
// demek. Consistent hashing hem düğümleri hem anahtarları bir HALKAYA
// (0..2^32) koyar; anahtar, saat yönünde ilk düğüme aittir. Bir düğüm
// eklenince yalnız o düğümün yayına düşen anahtarlar (~1/N) taşınır,
// gerisi yerinde kalır.
//
// Virtual node (vnode): bir düğümü halkada TEK noktaya koymak dengesiz
// yaylar (dolayısıyla dengesiz yük) verir. Her düğümü V noktaya
// dağıtmak, hem yükü hem de değişimdeki taşınmayı düzgünleştirir.
//
// Thread-safe DEĞİLDİR: topoloji değişimleri (Add/Remove) tek bir
// koordinatörden yapılmalı; eşzamanlı okuma gerekiyorsa çağıran
// senkronize etmeli (ya da anlık bir kopya alıp okumalı).
package hashring

import (
	"hash/crc32"
	"sort"
)

// Ring, consistent hashing halkasıdır.
type Ring struct {
	replicas int               // düğüm başına vnode sayısı
	ring     []uint32          // sıralı vnode konumları (binary search için)
	owner    map[uint32]string // vnode konumu → düğüm id'si
	nodes    map[string]bool   // kayıtlı düğümler (küme)
}

// New, düğüm başına replicas vnode'lu boş bir halka yaratır. replicas
// büyüdükçe dağılım dengelenir, bellek/GetN maliyeti artar (100-200
// tipik). replicas <= 0 ise 1 kullanılır.
func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = 1
	}
	return &Ring{
		replicas: replicas,
		owner:    map[uint32]string{},
		nodes:    map[string]bool{},
	}
}

func vnodeKey(node string, i int) string {
	// "node#i" — aynı düğümün farklı vnode'ları farklı konumlara düşer.
	return node + "#" + itoa(i)
}

func hash(s string) uint32 { return crc32.ChecksumIEEE([]byte(s)) }

// Add, düğümleri halkaya ekler (zaten varsa yok sayılır). Ekleme sonrası
// halka yeniden sıralanır.
func (r *Ring) Add(nodes ...string) {
	changed := false
	for _, node := range nodes {
		if node == "" || r.nodes[node] {
			continue
		}
		r.nodes[node] = true
		for i := 0; i < r.replicas; i++ {
			h := hash(vnodeKey(node, i))
			// Çakışma olursa (nadir) o vnode'u ilk sahibe bırak; kayıp
			// yalnız bir vnode, dağılımı bozmaz.
			if _, taken := r.owner[h]; !taken {
				r.owner[h] = node
				r.ring = append(r.ring, h)
			}
		}
		changed = true
	}
	if changed {
		sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
	}
}

// Remove, düğümü ve tüm vnode'larını halkadan çıkarır. Sahip olduğu
// anahtarlar saat yönündeki bir sonraki düğüme geçer; diğerleri
// etkilenmez.
func (r *Ring) Remove(node string) {
	if !r.nodes[node] {
		return
	}
	delete(r.nodes, node)
	kept := r.ring[:0]
	for _, h := range r.ring {
		if r.owner[h] == node {
			delete(r.owner, h)
			continue
		}
		kept = append(kept, h)
	}
	r.ring = kept
}

// Get, anahtarın ait olduğu düğümü döner: hash(key)'ten saat yönünde
// ilk vnode'un sahibi. Halka boşsa "" döner.
func (r *Ring) Get(key string) string {
	if len(r.ring) == 0 {
		return ""
	}
	h := hash(key)
	// İlk ring[i] >= h; yoksa halka başına sar.
	i := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if i == len(r.ring) {
		i = 0
	}
	return r.owner[r.ring[i]]
}

// GetN, anahtar için saat yönünde ilk n FARKLI düğümü döner (replika
// yerleşimi için: bir shard'ı n düğüme çoğaltmak). n mevcut düğüm
// sayısından büyükse tüm düğümler döner.
func (r *Ring) GetN(key string, n int) []string {
	if len(r.ring) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	h := hash(key)
	start := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	out := make([]string, 0, n)
	seen := map[string]bool{}
	for i := 0; i < len(r.ring) && len(out) < n; i++ {
		node := r.owner[r.ring[(start+i)%len(r.ring)]]
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
	}
	return out
}

// Nodes, kayıtlı düğümleri (sıralı) döner.
func (r *Ring) Nodes() []string {
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// itoa, küçük tamsayılar için hızlı; strconv bağımlılığı gerektirmez.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
