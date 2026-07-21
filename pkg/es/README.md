# pkg/es — Event Store

Faz 0'daki LSM motorunun (pkg/storage) üstünde append-only event
mağazası; Faz 2'nin CQRS/Event Sourcing temeli.

## Neden event sourcing?

Klasik CRUD mevcut durumu saklar ve üzerine yazar: "neden bu duruma
geldik?" sorusunun cevabı silinir. ES tersini yapar: GERÇEKLERİN sıralı
kaydı (kim ne dedi, ne topladı, ne takas etti) tek doğruluk kaynağıdır;
durum (read model'ler) bu kayıttan türetilir ve her an sıfırdan yeniden
kurulabilir. Bedeli: eventual consistency (read model bir tık geriden
gelir) ve şema evrimi disiplini (eski event'ler sonsuza dek okunabilir
kalmalı).

## Garantiler

- **Global toplam sıra**: her event benzersiz, kesin artan `Global`
  taşır; projection'lar "checkpoint'ten oku → uygula → sinyal bekle"
  döngüsüyle ilerler (`Subscribe` coalesced sinyal verir — actor
  mailbox'larındaki notify deseniyle aynı sözleşme).
- **Stream + optimistic concurrency**: her aggregate kendi stream'inde
  kendi `Seq`'iyle; `Append(stream, expectedVersion, ...)` yarışta
  `ErrVersionConflict` döner (saga'ların temeli).
- **Atomik batch**: bir Append'in tüm event'leri TEK storage kaydına
  yazılır — altta transaction yokken atomikliği "tek anahtar = tek WAL
  kaydı" sağlar. Crash'te batch ya tamamen vardır ya hiç.
- **Değişmezlik = okuma tutarlılığı (MVCC'nin ES hali)**: event'ler
  asla güncellenmediği için bir okuyucunun gördüğü [1..checkpoint]
  aralığı sonsuza dek aynıdır; versiyon, log pozisyonudur.

## Tasarım notları

- Stream indeksi ve versiyonlar RAM'de; açılışta log taranarak YENİDEN
  KURULUR. LSM'in MANIFEST'inin tersi bir karar: orada dosya listesi
  türetilemez olduğu için persist edilir; burada indeks logdan
  türetilebilir olduğu için edilmez — "neyi persist etmeli" sorusunun
  cevabı her zaman "türetilemeyeni".
- pkg/storage'a bu iş için `ScanFrom` (aralık taraması: skip list +
  sparse index seek) eklendi.
- Snapshot'lama (uzun stream'leri özetleme) ve log budama bilinçli
  kapsam dışı; oyuncu stream'leri büyüdüğünde (Faz 3+) gelecek.

## Learnings

- **Atomiklik sınırını veri düzeni belirler.** Transaction'sız motorda
  "batch = tek kayıt" kararı, atomikliği bedavaya getirdi; aynı veriyi
  üç anahtara yazsaydık crash tutarlılığı imkânsızdı.
- **Uzunluğa bakan test yarışlıdır.** Read model testi "len==100" ile
  erken dönüp yanlış pencereyi yakaladı; doğru senkron "son mesajı
  gördün mü"ydü — Faz 0'daki sinyal-tabanlı test dersinin devamı.
- **Kapanış idempotent olmalı.** Server.Stop'un çifte çağrısı (defer +
  test cleanup) panic'ledi; kapanış yolları her zaman çoğuldur,
  sync.Once şart.
