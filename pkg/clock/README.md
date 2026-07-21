# pkg/clock — Lamport ve Vector Clock

Faz 2+'da CRDT çakışma tespiti (vector) ve event sıralama kırıcısı
(Lamport) olarak kullanılacak.

## Neden mantıksal saat?

Fiziksel saatler dağıtık sıralama için güvenilmez: skew, NTP
sıçramaları, sanallaştırma duraksamaları. Lamport'un gözlemi: soru
"saat kaçta?" değil "hangisi önce?"dir ve cevabı nedenselliktir
(happens-before, →): aynı süreçte ardışıklık + mesaj gönderimi→alımı +
geçişlilik.

| | Lamport | Vector |
|---|---|---|
| Garanti | e1→e2 ⇒ L(e1)<L(e2) (tek yön!) | e1→e2 ⟺ V(e1)<V(e2) |
| Eşzamanlılık tespiti | YOK | VAR (Concurrent) |
| Maliyet | 1 uint64 | düğüm başına sayaç, O(N) |
| Kullanım | toplam sıra kırıcı (ts, nodeID) | çakışma/nedensellik analizi |

Uzantılar (bilinçli kapsam dışı): HLC (fiziksel zamana yakın + nedensel,
CockroachDB), TrueTime (donanım destekli belirsizlik aralığı, Spanner),
dotted version vector (istemci başına büyümeyi sınırlar, Riak).

## API sözleşmeleri

- `Lamport` thread-safe (atomic CAS); `Tick` yerel olay, `Observe`
  mesaj alımı: max(yerel, uzak)+1.
- `Vector` thread-safe DEĞİL: bir saat tek sürecin/aktörün durumudur
  (actor modelinde doğal). Mesaja `Clone()` iliştirilir; alımda
  `Merge` + `Tick`.
- Görünmeyen düğüm 0 sayılır: sabit üyelik listesi gerekmez, düğümler
  dinamik katılabilir.

## Learnings

- Lamport'un tek yönlü garantisi kolay unutulur: L(a) < L(b) nedensellik
  KANITLAMAZ; "eşzamanlıyı tespit et" gereksinimi çıktığı an vector
  clock (ve O(N) bedeli) kaçınılmazdır — CRDT'lerde tam bu gerekecek.
- Üç düğümlü senaryo testi (zincir nedensellik + çapraz eşzamanlılık)
  Compare'in dört sonucunu da tek hikâyede doğruluyor; property tarzı
  ("her → çifti Before dönmeli") testlerin küçük bir örneği.
