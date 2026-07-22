# pkg/dlock — Raft Üstünde Dağıtık Kilit

Lease tabanlı, fencing token'lı dağıtık kilit. Faz 3'te shard/handoff
gibi "aynı anda tek sahip" gerektiren işler için.

## Neden Raft? Neden CRDT değil?

Kilit, **anlaşma** problemidir: "kim tutuyor?" sorusunun TEK doğru cevabı
olmalı. CRDT bunu çözemez — CRDT'ler çelişkiyi birleştirir, seçmez. Bu
yüzden kilit CP tarafındadır ve replicated state machine ister: kilit
tablosu Raft log'undan deterministik türetilir, tüm replikalar aynı
sonuca varır.

Faz 2'deki CRDT sayaçla karşıtlık nettir: sayaç bölünmede çalışmaya
devam eder (AP), kilit **durur** (CP). Aynı sistemde iki farklı
tutarlılık profili bilinçli olarak bir arada.

## Üç tasarım kararı

1. **Lease (TTL).** Kilidi tutan çökerse kilit sonsuza takılı kalmamalı;
   süre dolunca başkası alır. Bedeli: tutan aslında yaşıyor ama
   duraklamışsa (GC, ağ) lease'i dolabilir → kısa bir "iki sahip"
   penceresi.

2. **Karar state machine'de, zaman liderden.** "Alabilir mi?" kararı
   apply sırasında verilir. Replikalar wall-clock kullansaydı her biri
   farklı sonuç hesaplardı (determinizm kaybı). Bu yüzden **lider kendi
   zaman damgasını komuta koyar**; tüm replikalar o damgayla aynı kararı
   üretir.

3. **Fencing token.** (1)'deki pencerenin doğru çözümü kilidin kendisi
   değil, **korunan kaynağın eski token'ı reddetmesidir**. `Acquire`
   monoton artan bir token döner (Raft log index'i — tasarım gereği
   monoton). Kaynağa her yazmada token taşınır; kaynak gördüğünden küçük
   token'ı reddeder. Kleppmann'ın klasik uyarısı: *lease tek başına
   yeterli değildir.* `Renew` token'ı DEĞİŞTİRMEZ (aynı sahiplik
   dönemi); yeni `Acquire` her zaman daha büyük token verir.

## Test edilen garantiler

- Karşılıklı dışlama; yalnız sahibi bırakabilir.
- Lease süresi dolunca devir + **token büyür** (fencing çalışır).
- Renew sahipliği ve token'ı korur, lease'i uzatır.
- 8 eşzamanlı istemciden **tam biri** kazanır.
- Tüm replikaların state machine'leri aynı sahip/token'da yakınsar.
- Çoğunluk yokken kilit hizmeti durur (`ErrNoQuorum`), `Holder` cevap
  vermez; iyileşince **kilit durumu korunur**.
- Lider failover'ında kilit durumu korunur.

## Yol üstünde bulunan Raft açığı

Bölünme sonrası "kilit kayboldu" hatası, aslında Raft'ın gerçek bir
inceliğiydi: **yeni lider, kendi döneminden bir kayıt commit edene kadar
önceki dönemlerin kayıtlarını commit edemez** (§5.4.2) — dolayısıyla
state machine'e uygulayamaz ve okuyamaz. Çözüm makalenin §8'de önerdiği
standart yol: lider göreve başlar başlamaz bir **no-op** kaydı ekler.
pkg/raft'a bu eklendi; state machine'ler boş `Cmd`'li kaydı yok sayar.

## Bilinçli sınırlamalar

- Kilit kümesi kendi Raft grubunu kurar (in-memory ağ); kalıcı depo
  (raftstore) bağlamak kolay ama şimdilik bağlanmadı.
- Linearizable read için lider lease'ine güveniliyor (`QuorumActive`);
  tam linearizability için ReadIndex/no-op-per-read gerekir.
- Sıraya girme (fair queueing / watch) yok: `Acquire` başarısızsa çağıran
  yeniden dener.
