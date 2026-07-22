# 2PC / 3PC ile Saga Karşılaştırması

Faz 2'de takası **saga** ile yaptık ([services/trade](../services/trade/README.md)).
Bu yazı "neden 2PC değil?" sorusunu, Shardlands'in kendi kodundan
örneklerle yanıtlar.

## Problem

İki oyuncunun envanteri (`inv-A`, `inv-B`) atomik değişmeli: ya takas
tamamen olur ya hiç olmaz. İki ayrı aggregate, tek transaction'a sığmaz.

## 2PC (Two-Phase Commit)

**Nasıl:** Koordinatör iki fazda ilerler.
1. *Prepare:* tüm katılımcılara "hazır mısın?" sorar; her katılımcı
   kaynağı **kilitler** ve "evet/hayır" der.
2. *Commit:* hepsi "evet" dediyse "commit" der, biri "hayır" derse
   "abort".

**Kazancı:** Gerçek atomiklik ve **izolasyon** — commit'ten önce hiçbir
ara durum görünmez. Tek bir kavramsal transaction.

**Bedeli:**
- **Bloklama:** Katılımcı "evet" dedikten sonra koordinatörü beklerken
  kaynağı kilitli tutar. Koordinatör prepare ile commit arasında çökerse
  katılımcılar **belirsiz (in-doubt)** kalır — kilitler koordinatör
  dönene kadar açılmaz. Bu, 2PC'nin meşhur "blocking protocol" eleştirisi.
- **Kullanılabilirlik:** Bir katılımcı erişilemezse tüm işlem durur.
  Katılımcı sayısı arttıkça başarı olasılığı çarpımsal düşer.
- **Ölçek:** Kilit süresi ağ turlarına bağlıdır; yüksek eşzamanlılıkta
  çekişme (contention) patlar.

## 3PC (Three-Phase Commit)

2PC'nin bloklamasını azaltmak için araya *pre-commit* fazı ekler:
koordinatör çökse bile katılımcılar birbirine bakarak sonucu
kararlaştırabilir (non-blocking, **senkron ağ varsayımıyla**).

**Neden yaygın değil:** Ek bir tam tur gecikme getirir ve **ağ bölünmesi**
varsayımını karşılamaz — asenkron/bölünebilir ağda 3PC de yanlış sonuç
üretebilir. Pratikte 2PC + sağlam koordinatör (veya konsensüs destekli
koordinatör) ya da saga tercih edilir.

## Saga (bizim seçimimiz)

**Nasıl:** Tek büyük transaction yerine, her biri kendi lokal
transaction'ı olan **adım dizisi** + her adımın **telafisi**. İleri
gidemezsen geri sararsın.

Shardlands'te takas adımları: A'nın malını rezerve et → B kabul → B'nin
malını rezerve et → çapraz transfer. Telafiler rezervasyonları geri verir.

**Kazancı:**
- **Bloklamaz:** Kimse "in-doubt" beklemez; her adım kendi başına
  tamamlanır. Katılımcı geçici erişilemezse saga telafi edip iptal eder.
- **Kullanılabilirlik ve ölçek:** Uzun süreli kilit yok; adımlar
  bağımsız, yeniden denenebilir.
- **Gerçek dünyaya uyar:** İnsan süreçleri de böyledir (sipariş → ödeme
  → kargo; iptal → iade).

**Bedeli:**
- **İzolasyon YOK:** Ara durumlar görünür. Bizde bunu "rezerve" durumu
  temsil eder: mal `available`'dan düşer, `reserved`'a geçer — dışarıdan
  "askıda" görünür. Kirli okuma riski, modeli açıkça bu şekilde
  tasarlayarak yönetilir.
- **Telafi yazmak zorundasın:** Ve telafiler *semantik*tir, "geri al"
  değil (parayı iade edersin, zamanı geri alamazsın).
- **Idempotentlik zorunlu:** At-least-once teslimle her adım
  tekrar-güvenli olmalı (bizde `tradeID` idempotency key).

## Karşılaştırma tablosu

| | 2PC | 3PC | Saga |
|---|---|---|---|
| Atomiklik | Gerçek | Gerçek | Sonuçta (telafiyle) |
| İzolasyon | Var | Var | **Yok** (ara durum görünür) |
| Bloklama | Var (in-doubt) | Azaltılmış | Yok |
| Bölünmeye dayanıklılık | Zayıf | Zayıf (senkron varsayım) | İyi |
| Gecikme | 2 tur + kilit | 3 tur | Adım sayısı kadar, kilitsiz |
| Karmaşıklık nerede | Protokolde | Protokolde | **Uygulama kodunda** (telafiler) |
| Shardlands kullanımı | — | — | **Takas** |

## Peki Raft neyi değiştirir?

Faz 3'te koordinasyon için Raft'ımız var. "Koordinatörü Raft'la
replike edip 2PC yapsak?" — bu gerçek bir seçenek (Spanner benzeri
sistemler tam olarak bunu yapar: 2PC + her katılımcı grubu Paxos/Raft
ile replike). Kazanç: koordinatör çökmesi artık bloklamaz.

Biz yine de saga'da kaldık, çünkü:
- Takas **oyun içi, kullanıcıya dönük** ve kilit tutmak gecikmeyi
  görünür kılar (PACELC'de E→L tercihimiz).
- Katılımcılar aynı süreçte değil, aynı **event log** üstünde; saga
  buraya doğal oturuyor.
- 2PC'nin izolasyon kazancına ihtiyacımız yok: "rezerve" durumunu zaten
  domain'de modelledik.

**Ama** Raft'ı ihtiyaç duyduğumuz yerde kullandık: shard sahipliği ve
[dağıtık kilit](../pkg/dlock/README.md) — yani "tek doğru cevap" gereken
seyrek kararlar. Doğru ders: *protokolü probleme göre seç; aynı sistemde
saga ve konsensüs bir arada yaşar.*
