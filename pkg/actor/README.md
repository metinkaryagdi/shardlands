# pkg/actor — Sıfırdan Actor Framework

Shardlands'te session yönetimi, world shard'ları ve arena tick loop'ları bu
framework üzerinde koşacak.

## Neden actor model?

Paylaşılan durum + lock yerine: her aktörün **özel state'i** vardır ve ona
yalnızca **kendi goroutine'i** dokunur. Dış dünya state'e asla değmez;
`Ref` üzerinden **mesaj** gönderir. Mesajlar tek tek, FIFO işlendiği için
aktör içinde senkronizasyon gerekmez — data race sınıfı hatalar tasarımla
yok edilir. MMO'da doğal karşılığı: her oyuncu session'ı, her world shard'ı,
her arena bir aktör.

**Alternatifler ve trade-off'lar:**

- *Mutex'li paylaşılan durum*: az sayıda yapı için basit; ama kilit sırası,
  ölü kilit ve "hangi lock neyi koruyor" sorunları büyüdükçe patlar.
- *Saf CSP (kanal boru hatları)*: Go'nun doğal stili; ama binlerce dinamik,
  isimlendirilmiş, denetlenen (supervised) varlık için yaşam döngüsü ve
  hata yönetimi sağlamaz — actor model tam bunu ekler.
- *Hazır kütüphane (proto.actor, ergo)*: üretimde mantıklı; burada amaç
  mekanizmayı sökerek öğrenmek.

Maliyeti: mesajlaşma dolaylılığı (debug'da stack trace yerine mesaj akışı
izlersin) ve istek/cevap akışlarının doğal olmaması.

## Tasarım

```
System ── guardian(/user) ── aktörler ── çocuk aktörler   (ağaç)

her aktör = process {
    goroutine          tek işleyici; state lock'suz
    user mailbox       bounded chan (Block | DropNewest)
    ctrl queue         unbounded, ÖNCELİKLİ (stop, escalation, child-stopped)
    actor instance     Producer() ile yaratılır, restart'ta yenilenir
}
```

Önemli kararlar:

1. **İki kuyruk.** `Stop` ve escalation gibi kontrol mesajları, dolu bir
   mailbox'ın arkasında beklememeli; loop her turda önce ctrl kuyruğuna
   bakar. Ctrl kuyruğu bilinçli olarak unbounded: framework'ün kendi
   protokol mesajları bloklanırsa deadlock doğar (ör. duran çocuğun
   "durdum" bildirimi, çocuklarını bekleyen ebeveyni kilitleyemez olmalı).
2. **Restart instance'ı değiştirir, process'i değil.** `Ref` ve mailbox
   kalıcıdır; `Producer` taze bir instance üretir. Böylece restart'tan
   sonra elde tutulan Ref'ler geçerli kalır ve kuyruktaki mesajlar
   kaybolmaz — state ise garantili sıfırlanır (Erlang'ın "let it crash"
   felsefesi: bozulmuş state'i onarmaya çalışma, temizden başla).
3. **Supervision deklaratif.** `Props.Supervision` aktörün kendi
   hatalarına uygulanacak kararı taşır: `Restart` (varsayılan, kayan
   pencerede limitli), `Resume`, `Stop`, `Escalate`. Limit aşımı restart
   fırtınasını keser. Escalate hatayı ebeveynin hatası yapar — hata
   yönetimi ağaçta yukarı taşınabilir.
4. **Aşağıdan yukarıya temizlik.** Stop/restart önce çocukları durdurup
   bekler, sonra `PostStop` çalışır; `Stopped()` kanalı kapandığında ağaç
   gerçekten temizlenmiştir (test senkronizasyonu için de happens-before
   garantisi verir).
5. **Dead letter.** Ölü aktöre gönderilen, taşan (DropNewest) veya stop
   anında mailbox'ta kalan mesajlar sayaca ve opsiyonel handler'a düşer —
   sessiz kayıp yok, gözlemlenebilir kayıp var.

## Bilinçli sınırlamalar (şimdilik)

- `ask/request-response` deseni yok (gerekince eklenecek).
- Stop, `Receive` içinde sonsuza dek bloklanan kullanıcı kodunu
  kesemez (zaman aşımı yok) — aktörler bloklamamalı.
- Mailbox artık [pkg/ringbuf](../ringbuf/README.md) üzerinde: lock-free
  MPSC ring buffer + cap-1 coalesced uyandırma sinyalleri (notify/space).
  Kanal sürümüne göre saf kuyruk maliyeti ~6× düştü; blok/bekleme
  semantiği sinyal katmanında korunuyor.
- AllForOne (kardeşleri birlikte restart) stratejisi yok; OneForOne var.

## Learnings

- **Öncelikli kontrol kanalı şart.** İlk sezgi tek mailbox'tı; "dolu
  kuyruğun arkasında Stop bekliyor" senaryosu iki kuyruk + öncelik
  gerektirdi. Akka'daki system mailbox ayrımının sebebi bu.
- **Unbounded'ın meşru kullanımı.** Kullanıcı mesajlarında backpressure
  (bounded + Block) doğru; framework iç protokolünde ise ilerleme
  garantisi kayıptan önemli — unbounded ctrl kuyruğu deadlock'ları kesti.
- **Kapanış yarışları.** "Ölü aktörün dolu mailbox'ına Block ile gönderen
  sonsuza dek takılır" hatası, gönderim select'ine `stoppedCh`
  eklenerek çözüldü. Kapanma sırası (önce `dead` bayrağı, sonra
  `close(stoppedCh)`, sonra drain) bilinçli; nadir bir yarışta mesaj dead
  letter sayılmadan düşebilir — dağıtık sistemlerde "exactly-once teslim
  yoktur" dersinin minyatürü.
- **Testlerde senkronizasyon sinyalle olmalı.** `sleep` tabanlı testler
  yerine kanal sinyalleri (`sums`, `processed`, `Stopped()`)
  determinizm sağladı; iki test ilk turda tam da eksik senkronizasyon
  yüzünden düştü.
