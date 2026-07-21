# pkg/ringbuf — Lock-Free MPSC Ring Buffer

Actor mailbox'ının altındaki veri yapısı; Faz 5'te arena tick loop'unda
da doğrudan kullanılacak.

## Neden lock-free, neden bu tasarım?

Mailbox'a N üretici yazar, tek tüketici (aktörün goroutine'i) okur.
Mutex'li bir kuyrukta her push/pop çekirdekler arası kilit trafiği
üretir; Go kanalı da içinde mutex taşır. Lock-free tasarımda üreticiler
yalnızca bir CAS için yarışır, tüketici hiç yarışmaz.

Tasarım **Vyukov bounded MPMC kuyruğunun** MPSC'ye sadeleştirilmiş hali.
Kilit fikir: doluluk/boşluk kararını paylaşılan head/tail karşılaştırması
yerine **slot başına sequence sayacı** verir — üretici ile tüketici
birbirinin pozisyon sayacını hiç okumaz, her biri yalnızca hedef slotun
seq'ine bakar:

```
seq == pos     slot boş, pos'taki üretici yazabilir
seq == pos+1   slot dolu, tüketici okuyabilir
seq == pos+C   slot boşaltıldı, bir sonraki turu bekliyor  (C = kapasite)
```

**Alternatifler:** mutex+deque (basit, contention'da çöker), Go kanalı
(ölçüm: ~6× yavaş, aşağıda), Michael-Scott unbounded linked queue
(bounded backpressure istiyoruz + node başına allocation), SPSC ring
(daha da hızlı ama üretici sayımız N). Trade-off: bounded olmak zorunda,
kapasite 2'nin kuvvetine yuvarlanır, tüketici tekliği çağıran tarafından
garanti edilmeli (API'de zorlanamıyor).

## İnce noktalar

- **Asgari kapasite 2.** cap==1'de `pos+1 == pos+C` olduğundan "dolu" ile
  "boşaltıldı" durumları aynı seq değerine çakışır ve kuyruk sessizce
  bozulur (yazılmamış değerin üzerine yazma → kilitlenme). Bunu testte
  canlı deadlock olarak yaşadık; `New` artık 2'ye yuvarlar ve
  `TestMinCapacityCycle` regresyonu tutar.
- **False sharing.** `enq` (üreticilerin CAS ile dövdüğü) ve `deq`
  (tüketicinin yazdığı) sayaçları 64B cache-line dolgusuyla ayrılır;
  aynı satırda kalsalar her CAS tüketicinin satırını geçersiz kılar.
  Dolgusuz kopya benchmark'ta ~%24 yavaş (aşağıda).
- **"Lock-free" dürüstlüğü.** Slotu CAS ile kapmış ama seq'i henüz
  yayınlamamış bir üretici askıya alınırsa tüketici o slotu geçemez;
  teorik anlamda tam lock-free değil, pratikte non-blocking.
- **Go bellek modeli.** `sync/atomic` işlemleri Go'da sequentially
  consistent; C++'taki acquire/release seçim yükü yok. `s.val = v`
  yazısının `s.seq.Store(pos+1)`'den önce görünür olması bu sayede
  garanti (store yayınlama noktasıdır).

## Benchmark (i7-13650HX, Go 1.26, `go test -bench . -benchtime 2s`)

| Kuyruk | ns/op |
|---|---|
| Ringbuf MPSC (dolgulu) | **173** |
| Ringbuf MPSC (dolgusuz, false sharing) | 215 |
| Go kanalı (buffered, aynı iş yükü) | 1022 |

Mikrobenchmark uyarısı: saf push/pop hızını ölçer; gerçek mailbox'ta
uyandırma sinyalleri ve mesaj işleme maliyeti baskındır.

## Learnings

- **Sınır durumu tasarımın parçası.** cap-1 çakışması kod gözden
  geçirmede değil, aktör testinin deadlock'unda yakalandı; invariant'ı
  ("dolu" ve "boşaltıldı" ayrık kalmalı) yazıya dökünce asgari kapasite
  kendiliğinden çıktı.
- **Poll tabanlı yapı + bloklayan API = sinyal katmanı.** Ring buffer
  bloklamaz; actor mailbox'ındaki Block/bekleme semantiği cap-1
  coalesced sinyal kanallarıyla (notify/space) kuruldu. "Sinyal mesaj
  değildir, 'tekrar dene' demektir" ayrımı lost-wakeup hatalarını önler.
- **Padding ölçülebilir.** False sharing soyut bir korku değil; aynı
  algoritmada yalnızca dolguyu kaldırmak bu makinede ~%24 kaybettirdi.
