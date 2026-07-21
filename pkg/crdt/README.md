# pkg/crdt — Çakışmasız Replika Veri Tipleri

G-Counter (yalnız artan) ve PN-Counter (artan + azalan). Faz 2'de global
"toplam toplanan kaynak" sayacı; Faz 3'te shard'lar arası yakınsama.

## Raft'ın tam karşıtı

| | Raft (pkg/raft) | CRDT (bu paket) |
|---|---|---|
| Amaç | Tek doğru SIRA üzerinde anlaşma | Anlaşma GEREKTİRMEYEN veri |
| Nasıl | Lider + çoğunluk oyu | Yerel yaz, sonra merge |
| Bölünmede | Azınlık ilerleyemez (C) | Herkes yazmaya devam (A) |
| CAP | CP | AP |
| Gecikme | Çoğunluk turu bekler | Yerel, anlık |
| Shardlands | Arena maç sonucu | Toplam kaynak sayacı |

CRDT'nin sırrı: merge işlemini bir **join-semilattice** yap —
commutative (a⊔b=b⊔a), associative, idempotent (a⊔a=a). O zaman
kopyalar mesajları hangi sıra/kaç kez alırsa alsın **aynı değere
yakınsar** (Strong Eventual Consistency). Koordinatör yok.

## Vector clock ile aynı kök

Bir G-Counter `map[node]sayaç`'tır ve merge'ü **eleman-bazlı max** —
bu, [pkg/clock](../clock/README.md)'taki vector clock'un merge kuralının
AYNISI. Fark yorumda: vector clock sonucu *karşılaştırır* (nedensellik/
eşzamanlılık), G-Counter *toplar* (değer). İkisi de aynı semilattice.
`TestGCounterMergeEqualsVectorClockMerge` bunu birebir doğrular: aynı
artışlardan kurulan G-Counter ve `clock.Vector`, merge sonrası aynı
haritaya sahip.

## PN-Counter: iki yönlü değer

G-Counter yalnız artabilir. Hem artıp azalan değer için hile: iki
G-Counter (P=artışlar, N=azalışlar), değer = P−N. İkisi de monoton
arttığından her biri CRDT kalır. Ekonomi bakiyeleri, oylar için ideal.
**Çevrimiçi sayısı için uygun DEĞİL**: bir düğüm LEAVE yazamadan çökerse
azalış kaybolur (sayaç şişer) — o yüzden gateway online sayısını basit
gauge tutar, CRDT değil.

## State-based (CvRDT) tercihi

Bu tipler tüm durumu gönderip merge eder (state-based). Operation-based
alternatif exactly-once teslim ister; state-based idempotentlik sayesinde
at-least-once/kayıplı ağlara doğal uyar — Faz 4 event bus için doğru
seçim.

## Test edilen özellikler

- Merge: commutative, associative, idempotent (hem G hem PN).
- Yakınsama: N replika bağımsız yazar, kaotik/tekrarlı gossip sonrası
  hepsi aynı değere ve aynı duruma yakınsar.
- Vector clock eşdeğerliği (yukarıda).
- PN değer negatife inebilir; merge değeri korur.

## Learnings

- **Matematik, koordinasyonu satın alır.** Merge'ü semilattice yapmanın
  bedeli küçük (max/topla), kazancı büyük: lider, çoğunluk, kilit
  olmadan yakınsama. "Doğru veri tipini seç, protokolü kaldır."
- **Her sayaç CRDT olmaz.** Monoton/kümülatif (toplam toplanan) mükemmel
  uyar; anlık/geri-alınabilir (çevrimiçi sayısı) uymaz. Aracı probleme
  göre seç — bunu online-gauge kararında bilinçle uyguladık.
- **Faz 0 tohumu Faz 2'de çiçek açtı.** Vector clock'un element-wise max
  merge'ü, burada değer sayacına dönüştü; aynı semilattice iki farklı işe.
