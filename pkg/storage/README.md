# pkg/storage — LSM-Tree Storage Engine

Sıfırdan yazılmış key-value motoru; Faz 2'de event store olacak.

## Neden LSM-tree?

B-tree tabanlı motorlar (InnoDB, bolt) güncellemeyi yerinde yapar:
her yazma diskte rastgele I/O'dur, okuma doğrudan ve hızlıdır. LSM
(Log-Structured Merge) tersini seçer: **tüm yazılar sıralı append'tir**
(WAL + değişmez SSTable dosyaları), okuma ise birden çok katmana bakmak
zorundadır. Event store iş yükümüz yazı-ağırlıklı ve append-doğal olduğu
için LSM doğru taraf.

Klasik üçlü trade-off (hepsini aynı anda düşüremezsin):

- **Yazma amplifikasyonu**: aynı kayıt compaction'larda tekrar tekrar
  yazılır.
- **Okuma amplifikasyonu**: bir Get, memtable + N tabloya bakabilir
  (bloom filter bunu yumuşatır).
- **Alan amplifikasyonu**: eski sürümler/tombstone'lar compaction'a
  kadar disk işgal eder.

## Mimari

```
Put/Delete ──► WAL (append + CRC) ──► memtable (skip list, sıralı RAM)
                                          │ dolunca flush
                                          ▼
                              SSTable (değişmez dosya):
                              [kayıtlar][sparse index][bloom][footer]
                                          │ sayı artınca compaction
                                          ▼
                              tek birleşik SSTable (tombstone'lar düşer)

Get:  memtable → tablolar (yeniden eskiye); ilk kayıt kazanır,
      tombstone "silinmiş" demektir. Bloom filter diske inmeyi keser.
MANIFEST: canlı tablo listesi; temp + atomik rename ile güncellenir.
```

Bileşen kararları:

- **Memtable = skip list** (LevelDB/RocksDB gibi): dengeli ağaç
  garantisine rotasyonsuz, yazı-tura ile ulaşan sıralı yapı.
- **SSTable**: her 16 kayıtta bir sparse index (RAM'de anahtar/16),
  10 bit/anahtar bloom filter (Kirsch-Mitzenmacher double hashing),
  kayıt başına CRC32. Değişmezlik sayesinde okumalar kilitsiz.
- **WAL ve SSTable aynı kayıt çerçevesini paylaşır**
  (`len|crc|payload`): tek codec, iki dayanıklılık probleminin (torn
  tail, bit çürümesi) tek çözümü.
- **Compaction: hepsini-birleştir** (full compaction). Tombstone
  düşürmek YALNIZCA tüm tablolar birleşirken güvenli — kısmi
  compaction'da tombstone atmak eski değeri "diriltir". Leveled /
  size-tiered stratejiler bilinçli ertelendi (yazma amplifikasyonu
  büyüyünce gerekecek).
- **MANIFEST = tek doğruluk kaynağı.** Bir .sst ancak manifest'te
  listeleniyorsa vardır. Flush sırası: tablo yaz+fsync → manifest
  (atomik rename) → WAL sıfırla → taze memtable. Her adım arasındaki
  crash için davranış tanımlı (yarım dosya açılışta silinir, WAL replay
  idempotent).

## Crash/chaos testleri

- WAL replay: flush edilmemiş yazılar ve tombstone'lar crash sonrası
  geri gelir.
- Torn tail: yarım yazılmış son WAL kaydı sessizce atılır, öncesi
  kurtulur.
- Orphan .sst: manifest'e girmeden crash olmuş dosya açılışta silinir,
  içeriği sızmaz.
- Resurrection: silinen anahtar compaction + reopen sonrası geri
  dirilmez.
- Bit çürümesi: SSTable'da tek bayt bozulması `ErrCorrupt` üretir.

## Bilinçli sınırlamalar

- MVCC yok: okumalar RWMutex altında; uzun Scan yazıları bekletir
  (Faz 2'de snapshot okumaya evrilecek).
- Tek compaction stratejisi (full), arka plan compaction yok
  (senkron, deterministik — test edilebilirlik önceliği).
- Aralık silme (range tombstone), snapshot, TTL yok.

## Benchmark (i7-13650HX, sync=false)

| İşlem | ns/op |
|---|---|
| Put (16B key, 100B value) | ~10 600 |
| Get (100k anahtar, tablodan) | ~15 300 |

## Learnings

- **Sıralama = dayanıklılık.** "Önce WAL sonra memtable", "önce fsync
  sonra manifest", "önce manifest sonra eski dosyaları sil" — motorun
  doğruluğu veri yapılarından çok bu üç sıralamada yaşıyor. Her birinin
  arasına "burada crash olursa ne olur?" sorusu soruldu ve testlendi.
- **Tombstone düşürme kuralı.** "Silineni at" sezgisi ancak gölgede
  eski katman kalmadıysa doğru; aksi resurrection. Full compaction bu
  yüzden en güvenli başlangıç stratejisi.
- **Windows dosya semantiği.** Açık dosya silinemez (compaction önce
  reader kapatır); yeni kapatılmış dosyayı silmek antivirüs yüzünden
  geçici takılabilir (WAL reset artık silme değil O_TRUNC). POSIX
  varsayımları taşınabilir değil.
- **Flaky test hediyedir.** Storage testleri koşarken actor'ün kapanış
  sıralamasındaki gerçek bir açık (drain'den önce close(stoppedCh))
  beşinci koşuda yakalandı ve düzeltildi.
