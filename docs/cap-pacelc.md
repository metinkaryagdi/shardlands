# CAP / PACELC Deneyi — Bilinçli İzolasyon

Bu yazı, Shardlands'te **bilerek** bir shard'ı izole ettiğimizde ne olduğunu
ve bunun CAP/PACELC ile ilişkisini belgeler. Deney kodda yaşıyor:
[services/server/cap_test.go](../services/server/cap_test.go).

## Kurulum

- Dünya 2×2 = 4 **bölgeye** ayrık; her bölge bir aktör.
- Consistent hashing bölgeleri **shard node**'lara atar (varsayılan iki
  shard).
- Her shard bir **Raft grubudur** (3 replika, kendi yalıtık ağı).
  Shard'ın bölgelerini simüle etme yetkisi grubun **liderine** aittir.
- Kullanılabilirlik tanımı kritiktir: "bir düğüm kendini lider sanıyor"
  YETMEZ. `raft.Node.QuorumActive` ile **çoğunlukla teması süren** lider
  aranır. Bölünmüş lider commit edemez; bunu ayırt etmezsek "kullanılabilir"
  yanılgısına düşeriz.

## Deney

`Group.IsolateAll()` ile bir shard'ın üç replikası birbirinden ayrılır:
hiçbir tarafta çoğunluk kalmaz.

### Gözlem 1 — CP tarafı: bölge donar

Shard kullanılamaz hâle gelince o shard'ın bölgeleri **donar**: tick
işlenmez, snapshot yayınlanmaz, komut (hareket/sohbet/toplama) kabul
edilmez. İstemci bağlı kalır ama dünya ilerlemez.

> Tutarlılık için kullanılabilirlikten vazgeçildi. İki süreç aynı bölgeyi
> simüle edip çelişkili durum üretmektense, hiç ilerlememeyi seçiyoruz.

Alternatif olsaydı ne olurdu? Azınlıktaki lider simülasyona devam etseydi,
bölünme iyileştiğinde iki farklı "gerçek" olurdu (split-brain): oyuncu iki
yerde birden, toplanan kaynak iki kez sayılmış. Oyun durumunda bu, geri
alınamaz bir tutarsızlıktır.

### Gözlem 2 — AP tarafı: CRDT sayaç çalışmaya devam eder

Aynı bölünme sırasında `/api/stats` **cevap vermeye devam eder**: toplam
toplanan kaynak G-Counter'dır, anlaşma gerektirmez. Her replika kendi
bileşenini bağımsız artırır, merge (eleman-bazlı max) sonradan yakınsar.

> Aynı sistemde iki farklı tutarlılık profili: **bölge simülasyonu CP,
> global sayaç AP.** Doğru soru "sistem CP mi AP mi" değil, "hangi veri
> hangi tarafta olmalı".

### Gözlem 3 — Blast radius sharding ile sınırlı

Bir shard'ın izolasyonu diğer shard'ın bölgelerini etkilemez (gruplar
yalıtık ağlarda). Sharding yalnız ölçek değil, **arıza yalıtımı** da
sağlar: dünyanın yarısı çalışmaya devam eder.

### Gözlem 4 — İyileşme

`Heal()` sonrası grup yeni lider seçer, bölge donukluktan çıkar ve
simülasyon kaldığı yerden sürer. Oyuncunun bağlantısı hiç kopmaz.

## PACELC: bölünme yokken de bir takas var

CAP yalnız **bölünme anını** anlatır. PACELC devamını söyler:

> **P**artition ise **A** mı **C** mi; **E**lse (bölünme yokken) **L**atency
> mi **C**onsistency mi?

Shardlands'in profili: **PC/EL** — bölünmede tutarlılık (bölge donar),
bölünme yokken gecikme (düşük latency) önceliği.

Bunun somut karşılığı:
- Oyuncu hareketi Raft'tan GEÇMEZ. Her tick'i replike etseydik her
  hareket bir çoğunluk turu beklerdi (20Hz'de imkânsız). Hareket, shard
  liderinin yerel otoritesiyle işlenir — **E → L**.
- Raft yalnız **kimin lider olduğu** (shard sahipliği) ve kilitler için
  kullanılır: nadir, ama doğruluğu kritik kararlar — **P → C**.
- Kalıcı gerçekler (sohbet, toplama, takas) event log'a yazılır; okuma
  tarafı (read model) **eventual consistent**tir — yine E → L.

Bu ayrım tesadüf değil, tasarım: **sık ve gecikmeye duyarlı olan yerelde,
seyrek ve doğruluğu kritik olan konsensüste.**

## Dersler

- **"Lider sanıyorum" ≠ "hizmet verebiliyorum".** Bölünmüş lideri ayırt
  etmek için lease/quorum kontrolü (QuorumActive) şart; yoksa
  kullanılabilirlik ölçümü yalan söyler.
- **Donmak bir özelliktir.** Kullanıcı için kötü görünür ama split-brain'e
  yeğdir; sistemin hangi durumda ne yapacağını *önceden seçmek* CAP'in
  pratikteki anlamıdır.
- **CAP bir anahtar değil, bir bütçedir.** Aynı üründe veri parçası
  başına farklı taraflar seçilebilir (bölge=CP, sayaç=AP) ve doğrusu
  budur.
