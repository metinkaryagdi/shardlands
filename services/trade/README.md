# services/trade — Takas Saga'sı (Koreografi vs Orkestrasyon)

İki oyuncu arasındaki takası SAGA olarak uygular. Aynı iş **iki farklı
koordinasyon stiliyle** yazılıdır ki fark somut görülebilsin.

## Neden saga? (dağıtık transaction'ın yokluğu)

Takas iki envanteri atomik değiştirmeli: A'nın odunu B'ye, B'nin kristali
A'ya. Ama iki ayrı aggregate (inv-A, inv-B) tek bir transaction'a
sığmaz — dağıtık sistemlerde 2PC pahalı ve kırılgandır (koordinatör tek
hata noktası, kilitler bloklar). Saga bunun yerine: **her biri kendi
lokal transaction'ı olan bir adım dizisi + her adımın telafisi**. İleri
gidemezsen geri sararsın; sonuç ya tam başarı ya tam geri alınmış olur
(hiç "yarım takas" kalmaz).

Adımlar: A'nın malını rezerve et → B kabul → B'nin malını rezerve et →
çapraz transfer. Telafiler tutulan rezervasyonları geri verir.

## İki mekanizma, tek fark

Her ikisi de `steps.go`'daki AYNI adım mekaniğini kullanır (reserve/
settle/release). Fark yalnızca **kimin ne zaman karar verdiği**:

| | Orkestrasyon (`orchestrator.go`) | Koreografi (`choreographer.go`) |
|---|---|---|
| Kontrol | Tek koordinatör, lineer `Execute` | Merkez yok; event'lere tepki |
| Akışı okumak | Tek fonksiyonda yukarıdan aşağı | Handler'lara dağılmış (onProposed, onAccepted…) |
| Telafi | Adımın hemen yanında, görünür | İlgili tepkinin içinde, dağınık |
| Karşı taraf kararı | Senkron `Decider` çağrısı | `Accept`/`Reject`/`Expire` EVENT'i |
| Bağ(coupling) | Koordinatör tüm katılımcıları bilir | Katılımcılar birbirini bilmez, log'u bilir |
| Yeni adım eklemek | Fonksiyonu değiştir (tek yer) | Yeni handler + event (dağınık ama izole) |
| Gözlemlenebilirlik | Akış koddan okunur | Akışı event log'undan çıkarırsın |

**Ne zaman hangisi?** Az sayıda, sıkı sıralı adım ve net sahiplik →
orkestrasyon (okunur, borç düşük). Çok sayıda gevşek bağlı reaktif
katılımcı, bağımsız evrilmesi gereken servisler → koreografi. Gerçekte
ikisi karışık kullanılır; bu proje ikisini de aynı domain üstünde
gösteriyor.

## Ortak sağlamlık: idempotentlik (at-least-once ile yaşamak)

Saga adımları **en az bir kez** çalışabilir: koreografi koordinatörü
restart'ta event'leri baştan oynatır; "rezerve ettim ama işaretlemeden
çöktüm" boşluğu vardır. Bu yüzden:

- Envanter işlemleri (`inventory.Reserve/Release/Commit/Receive`)
  **tradeID ile idempotent**tir: aynı adım iki kez çağrılırsa ikincisi
  no-op'tur (idempotency key deseni).
- Koreografi tepkileri, iş yapmadan önce trade fazını fold'layıp
  **guard** eder — yanlış fazda no-op.
- `Reserve` ayrıca **optimistic concurrency**'lidir (bkz. inventory):
  AYRI takasların aynı malı kapışması çifte harcamaya yol açmaz.

`TestChoreographyRestartIdempotent`: saga bittikten sonra yeni bir
koordinatör tüm log'u yeniden oynatır; hiçbir yeni envanter event'i
eklenmez, bakiye tutarlı kalır.

## Test edilen telafi senaryoları

Her ikisi (orkestrasyon + koreografi) için:
- **Mutlu yol**: iki envanter çapraz geçer, rezervasyon kalmaz.
- **Reddetme**: karşı taraf reddeder → A'nın rezervasyonu geri alınır.
- **Timeout**: süre dolar → A'nın rezervasyonu geri alınır.
- **Karşı taraf yetersiz**: B'nin malı yetmez → A geri alınır.
- **Settle çökmesi**: transfer anında hata → HER İKİ rezervasyon geri.
- (Ayrıca tam yığın e2e: iki oyuncu toplar, `/api/trade` ile takas eder;
  canlı tarayıcıda da doğrulandı.)

## Learnings

- **Rezervasyon, dağıtık atomikliğin anahtarı.** "Önce tut, sonra
  taahhüt/telafi" olmadan iki taraflı takas ya çifte harcar ya tutarsız
  kalır. Rezervasyon = kilitsiz, event'le ifade edilen "geçici söz".
- **Idempotentlik saga'nın vergisidir.** At-least-once teslimle yaşamak,
  her adımı tekrar-güvenli yapmayı gerektirir; idempotency key (tradeID)
  bunun standart yolu. Bunu atlarsan restart bakiyeyi bozar — testte
  gördük.
- **Koreografinin sıra tehlikesi gerçek ama tek-projection serbestletir.**
  "Accept, ProposerReserved'dan önce gelirse kaybolur mu?" endişesi;
  tek-goroutine projection her event'i tam işleyip devam ettiği için
  fold hep güncel durumu görür — kayıp olmuyor. Çok tüketicili gerçek
  bir bus'ta (Faz 4) bu tekrar düşünülecek.
