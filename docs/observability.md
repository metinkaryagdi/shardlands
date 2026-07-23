# Gözlemlenebilirlik — üç sinyal, bir zincir, birkaç karar

Faz 7'nin bütünü. Kod tarafı `pkg/metrics`, `pkg/trace`, `pkg/logging`;
kurallar `deploy/k8s/obs/`.

## 1. Üç sinyalin iş bölümü

Yaygın hata "her şeyi her yere koymak"tır. Üçünün farklı maliyeti ve
farklı sorusu var:

| Sinyal | Soru | Maliyet | Kardinalite |
| --- | --- | --- | --- |
| Metrik | "ne kadar, ne oranda bozuk?" | ucuz, sürekli | **sıkı sınırlı** |
| Log | "bu olayda ne oldu?" | orta | serbest |
| Trace | "bu istek nerede durdu?" | pahalı | örneklenir |

Kural: **kimlik taşıyan hiçbir alan metriğe girmez.** `player_id`
etiketi, oyuncu sayısı kadar zaman serisi demektir ve o seriler
oyuncular gittikten sonra da bellekte kalır. Kimlik başına soru
log'un ve trace'in işidir.

## 2. Zincir: grafikten sebebe

Üç sinyal ayrı ayrı yarım işe yarar. Hata ayıklama yolculuğu şudur:

```
metrik → "login p99 fırladı"                 (bir GRAFİK gördün)
trace  → "şu istek player'da 3sn durdu"      (bir İSTEK buldun)
log    → "o istekte izin reddedildi"          (SEBEBİ okudun)
```

İkinci oktan üçüncüye geçişi sağlayan tek alan `trace_id`'dir. Kümede
kanıtlandı — hata anında düşen gerçek satır:

```json
{"level":"ERROR","msg":"create player failed","service":"gateway",
 "trace_id":"707bac3e3315a911529f36b3b8c532d2",
 "err":"rpc error: code = PermissionDenied ... unauthorized request on route"}
```

Bu satır, aynı `trace_id`'yi taşıyan trace'e ve o trace'i üreten metrik
sıçramasına bağlıdır. Zincir kapalı.

## 3. SLI seçimi: kullanıcının olduğu yerden ölç

Faz 7'nin en öğretici ölçümü şuydu:

```
gRPC sunucu tarafı (player içinde)   475 µs
gRPC istemci tarafı (hub → player)  7750 µs
giriş uçtan uca (kullanıcının gördüğü) 7225 µs
```

Yalnız sunucu tarafını ölçen bir sistem 475 µs görür ve "her şey
yolunda" der; kullanıcının beklediği süre onun **16 katıdır**. Aradaki
fark ağ ve iki mesh proxy'sidir — ve bu, tek bir trace'te ölçülerek
doğrulandı (8.958 ms istemci span'ı, 0.031 ms sunucu span'ı).

**Kural: SLI, ölçmesi kolay olan yerden değil, kullanıcının olduğu
yerden seçilir.** Bu yüzden gecikme SLO'su `login_duration` üstünden
tanımlı (gateway'in uçtan uca ölçümü), `grpc_server_duration` üstünden
değil.

## 4. SLO'lar ve payda kararı

| SLO | Hedef | Ölçülen taban |
| --- | --- | --- |
| Giriş kullanılabilirliği | %99.9 / 30 gün (~43 dk bütçe) | — |
| Giriş gecikmesi | p95 < 20 ms, p99 < 50 ms | p95 8.0 ms, p99 9.6 ms |
| Hub tick | p95 < 10 ms (bütçenin %20'si) | p95 47.5 µs |
| Arena tick | p95 < 6.6 ms (bütçenin %20'si) | (bkz. §5) |

### Payda kararı SLO'nun en tartışmalı kısmıdır

Giriş sonuçları beş etikete ayrılıyor ve **hepsi hata sayılmıyor**:

| Sonuç | SLO'da | Gerekçe |
| --- | --- | --- |
| `ok` | başarı | — |
| `client_error` | **kapsam dışı** | Kullanıcı geçersiz istek gönderdi; servis doğru davrandı. Hataya saymak, kötü istemcilerin SLO'muzu bozmasına izin vermektir. |
| `rate_limited` | **kapsam dışı** | Kötüye kullanım koruması tasarlandığı gibi çalışıyor. |
| `shed` | **hata** | Devre kesici/bulkhead doldu; meşru istek karşılanamadı. Kullanıcı açısından kesintidir. |
| `error` | **hata** | — |

Ayrım şu cümleyle özetlenir: *"koruma mekanizmalarının kasten
reddettiği"* ile *"kapasitemiz yetmediği için reddettiğimiz"* aynı şey
değildir.

### Tick eşiği neden bütçenin kendisi değil?

Hub 20Hz'de koşuyor → tick bütçesi 50 ms. Eşiği 50 ms yapmak
"iş zaten bitmişken alarm çalsın" demektir. Eşik bütçenin **%20'si**
(10 ms): dünya yavaşlamaya başladığında, oyuncular donuklaşmayı
hissetmeden önce haber verir.

Ölçülen taban 47.5 µs — bütçenin **binde biri**. Faz 0'dan beri yapılan
mikro-optimizasyonların bıraktığı marjın somut karşılığı bu.

## 5. Arena: kısa ömürlü iş yükü çekme modeline uymuyor

Arena Pod'u tipik olarak 90 saniye yaşıyor. Prometheus 15 saniyede bir
çekiyor → en iyi ihtimalle ~6 örnek, ve **Pod son çekimden sonra
ölürse o aralığın verisi tamamen kaybolur**.

Bu, pull modelinin bilinen sınırı. Standart çözümler:

- **Pushgateway**: kısa ömürlü işler metriklerini iter, Prometheus
  gateway'den çeker. Bedeli: gateway tek hata noktası olur ve `up`
  metriğinin bedava gelen "hedef ayakta mı" sinyali kaybolur.
- **Uzak yazma (remote write)**: süreç doğrudan uzak depoya yazar.
- **Toplamayı yukarı taşımak**: arena tick'ini arenada değil, kareleri
  vekil eden gateway'de ölçmek.

Bu kurulumda **hiçbiri yapılmadı**; arena Pod'ları normal şekilde
çekiliyor ve eksik örnekleme kabul ediliyor. Sebep dürüstçe: arena tick
SLO'su bu projede kritik değil (maç kısa, düşerse operator yeniden
yaratıyor) ve Pushgateway'in getireceği karmaşıklık öğretici değeri
kadar bedel taşıyor. Alarm `severity: ticket` — çağrı cihazı değil.

**Karar kaydı olarak buradadır**: eksik ölçümü "ölçülüyor" gibi
göstermek, Faz 6'da üç kez düştüğümüz "panoda doğru görünmek" tuzağı
olurdu.

## 6. Alarm tasarımı: çoklu pencere, çoklu yanma hızı

**Yanma hızı (burn rate)** = gerçekleşen hata oranı / bütçe oranı.
1x yanma bütçeyi tam 30 günde bitirir; 14.4x yanma bütçenin %2'sini
1 saatte yakar.

| Yanma | Pencereler | Şiddet | Anlamı |
| --- | --- | --- | --- |
| 14.4x | 1h + 5m | page | Bütçenin %2'si 1 saatte |
| 6x | 6h + 30m | page | Bütçenin %5'i 6 saatte |
| 1x | 6h + 30m | ticket | Yavaş sızıntı; gece uyandırmaz |

### Neden iki pencere birden?

- Tek **uzun** pencere yavaş tepki verir: sorun başlayalı yarım saat
  olmuştur.
- Tek **kısa** pencere gürültülüdür: anlık bir sıçrama çağrı cihazını
  çaldırır.

İkisini `and` ile bağlamak ikisini de çözer: uzun pencere *"gerçekten
yanıyor mu"*, kısa pencere *"hâlâ yanıyor mu"* sorusunu cevaplar. Sorun
kendini düzelttiyse kısa pencere hızla düşer ve **alarm kendiliğinden
susar** — insan müdahalesi gerekmez.

### Ham sayaca alarm kurulmaz

Bütün kurallar `rate()`/`increase()` üstünden. Bu bir stil tercihi
değil: Faz 7'de canlı yaşandı — hub Pod'u yeniden başlayınca sayaçlar
sıfırlandı ve gönderilen istekler "kaybolmuş" göründü. `rate()`
sayaç sıfırlanmasını tanır ve doğru hesaplar; ham değere bakan her
kural o anda yanılırdı.

## 7. Alarmların gerçekten çalıştığı doğrulandı

Yazılan alarm, ateşlendiği görülmeden bitmiş sayılmaz. Kümede
yapılan deney: player'ın mesh yetkilendirme politikası silindi
(Faz 6'nın kaos aracı yeniden kullanıldı).

```
sonuç dağılımı:  ok 13 | error 9 | shed 20 | rate_limited 0
hata oranı (5m): %37.7        (bütçe eşiği %0.1, 14.4x eşiği %1.44)
alarm durumu:    FIRING  LoginErrorBudgetBurningFast   (severity=page)
                 PENDING LoginErrorBudgetBurning
                 PENDING LoginErrorBudgetSlowBurn
```

İki yan gözlem:

- **`error` 9 → `shed` 20**: dokuz hatadan sonra Faz 4'ün devre
  kesicisi açıldı ve kalan istekler hızlı-başarısız oldu. Kesici
  metriklerde ilk kez görünür halde çalıştı.
- Hata anındaki log satırı `trace_id` taşıyordu (§2) — yani alarmdan
  sebebe giden zincir bu deneyde uçtan uca koştu.

### Ve alarm kendiliğinden sustu

Politika geri konduktan sonra:

```
5m hata oranı: %0.00
alarm durumu:  LoginErrorBudgetBurningFast → SUSTU (FIRING'den düştü)
               LoginErrorBudgetBurning     → PENDING (6h/30m hâlâ dolu)
               LoginErrorBudgetSlowBurn    → PENDING
```

Çoklu pencerenin asıl vaadi buydu: sorun çözülünce **kısa pencere
hızla düşer ve alarm insan müdahalesi olmadan susar**. Uzun pencereli
alarmlar bir süre daha bekler — çünkü bütçe gerçekten yandı ve bunu
unutmamaları gerekir.

## 8. Ölçmediklerimiz (bilinçli)

- **WS komut akışı izlenmiyor.** 20Hz girdi akışında span açmak faydalı
  sinyali gürültüye boğardı; ayrıca trace istek-cevap ağacı varsayar,
  WS oturumu uzun ömürlü akıştır. O yolun sağlığı metriklerle izleniyor
  (`dead_letters` = doygunluk, `ws_sessions` = kullanım).
- **Log toplayıcı yok.** Loglar JSON ve `service`/`trace_id` alanlı,
  yani Loki/ES'e hazır — ama bu kurulumda toplayıcı kurulmadı;
  `kubectl logs` ile okunuyor.
- **İzleme arka ucu yok.** Span'lar süreç içi halka tamponda; hub ve
  player ağaçları `/debug/traces` üstünden ayrı ayrı okunuyor. Gerçek
  kurulumda ikisi de OTLP ile Jaeger/Tempo'ya gönderir ve ağaç orada
  birleşir.
- **Alertmanager yok.** Alarmlar Prometheus'ta değerlendiriliyor ama
  bir yere yönlendirilmiyor. `severity: page|ticket` etiketleri o
  yönlendirmenin bağlanacağı yeri işaretliyor.

Dördü de "yapılmadı" olarak yazılı; hiçbiri "yapıldı" gibi
gösterilmiyor.
