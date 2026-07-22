# services/arena — Geçici Dövüş Instance'ları

Talep üzerine açılan, 60-90 saniyelik 1v1/2v2 maçlar. Hub'la aynı
platformda ama **bilinçli olarak farklı bir profil**.

## İki profil, tek platform

| | hub (services/world) | arena (bu paket) |
|---|---|---|
| Öncelik | Tutarlılık | **Gecikme** |
| Tick | 20 Hz | 30 Hz |
| Girdi yolu | Komut başına **aktör mesajı** | **Lock-free ring buffer**, tick başına toplu boşaltma |
| Durum | Kalıcı (event log) | **Geçici**; yalnız SONUÇ kalıcılaşır |
| Sahiplik | Shard + Raft (çoğunluk) | Tek instance, kısa ömürlü |
| Aşırı yükte | Backpressure (Block) | **Komut düşür** (eskimiş girdi işe yaramaz) |

Bu ayrım projenin ana tezidir: *aynı sistemde farklı veri/etkileşim
sınıfları farklı tutarlılık ve gecikme bütçeleri hak eder.*

## Neden aktör değil?

Hub'da her komut bir mailbox turudur — basit ve yeterli. Arena'da
30Hz × N oyuncu × girdi değişimi sıcak yoldur; burada **frame başına tek
boşaltma** daha uygun: oturum goroutine'leri Faz 0'daki
[pkg/ringbuf](../../pkg/ringbuf/README.md) MPSC kuyruğuna kilitsiz
yazar, tick döngüsü tek tüketici olarak toplu okur. Kuyruk dolarsa komut
**düşer** — arena'da eskimiş girdiyi beklemek, atmaktan kötüdür.

Simülasyon durumuna yalnız tick döngüsü dokunur (kilitsiz); dışarıya
açılan snapshot ve sonuç RWMutex ile korunur.

## Benchmark (i7-13650HX, Go 1.26)

```
BenchmarkTick-20                    39.81 ns/op    # 2v2 tam adım (girdi+fizik+çarpışma+snapshot)
BenchmarkInputPush-20               35.60 ns/op    # paralel üreticilerden kilitsiz push
BenchmarkArenaCountersUnpadded-20   16.05 ns/op    # komşu sayaçlar: FALSE SHARING
BenchmarkArenaCountersPadded-20      1.25 ns/op    # cache-line dolgulu: 12.8× hızlı
```

### False sharing: 12.8× fark

Senaryo gerçek: tek makinede N arena koşar, her biri **kendi** sayacını
(tick/isabet/düşen girdi) günceller. Sayaçlar bir dilimde yan yana
durursa 8 tanesi tek 64B önbellek satırına düşer; farklı çekirdeklerdeki
goroutine'ler **mantıksal olarak hiçbir şey paylaşmadıkları hâlde**
birbirinin satırını sürekli geçersiz kılar.

Faz 0'da ring buffer'ın `enq`/`deq` sayaçlarında bunu %24'lük bir farkla
görmüştük; burada çekişme 8 çekirdeğe yayıldığı için etki **12.8 kat**.
Ders aynı, ölçek farklı: *paylaşmadığını sandığın şeyi donanım paylaşıyor
olabilir.*

## Maç kuralları

- Takımlar karşılıklı kenarlarda doğar; WASD ile hareket, nişan yönüne
  ateş (~333ms bekleme).
- Mermi düşmana değerse 12 hasar; dost ateşi yok.
- Bir takım tamamen elenirse maç biter; süre dolarsa toplam canı fazla
  olan kazanır, eşitse beraberlik.
- Bitişte `OnEnd` bir kez çağrılır → oyuncular hub'a döner (handoff).

## Bilinçli sınırlamalar

- İstemci tarafı tahmin (client-side prediction) ve lag compensation
  yok: sunucu-otoriter, snapshot tabanlı.
- Yeniden doğma (respawn) yok — eleme, net bir bitiş verir.
- Arena durumu replike edilmez: kısa ömürlü ve düşük değerli; instance
  ölürse maç iptal olur (sonuç yazılmaz).
