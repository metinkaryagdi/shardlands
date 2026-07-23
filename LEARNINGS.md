# Shardlands — Sekiz Fazın Dersleri

Bu belge fazları özetlemez ([README](README.md) onu yapıyor). Burada
toplananlar, **sekiz faz boyunca farklı kılıklarda tekrar tekrar çıkan
ilkeler**. Tekrarı görünür kılmak, tek tek fazları anlatmaktan daha
öğretici oldu: aynı ders üç ayrı katmanda karşınıza çıktığında, onun
bir teknoloji ayrıntısı değil bir düşünme biçimi olduğunu anlıyorsunuz.

Derslerin çoğu **hata yaparak** öğrenildi; her birinin altında o hatanın
nasıl göründüğü ve nasıl bulunduğu yazılı.

| # | Ders | Tek cümlede |
| --- | --- | --- |
| [1](#1-söylediğine-güvenme-kanıtı-iste) | Kanıt iste | İddiayı, iddia edenin kendisi doğrulamaz |
| [2](#2-panoda-doğru-görünmek-çalışmak-değildir) | Panoda yeşil ≠ çalışıyor | Beş kez düştüm; en sinsisi "0 kesinti" diye okunan bilinmezlikti |
| [3](#3-süreç-içi-durum-süreçten-uzun-yaşayan-durumla-buluşunca-patlar) | Gizli durum | "Veritabanı yok" ≠ durumsuz; sayaç da durumdur |
| [4](#4-sınır-durumları-tasarımın-parçasıdır-sonradan-eklenen-yama-değil) | Sınır durumları | Sonradan bulunursa, genelde üretimde bulunur |
| [5](#5-türetilebilen-türetilir-türetilemeyen-persist-edilir) | Ne saklanır | Her şeyi kalıcı yapmak da bir hatadır |
| [6](#6-aynı-sistemde-iki-farklı-tutarlılıkgecikme-profili-yaşayabilir) | İki profil | "Hangi mimari doğru" değil, "hangi iş yükü için" |
| [7](#7-bağımlılık-arızasında-kendini-öldürme) | Yumuşak bağımlılık | Arızayı yut **ve** yuttuğunu görünür kıl |
| [8](#8-gözlem-katmanı-gözlediği-şeyi-değiştirmemeli) | Gözlemci etkisi | Ölçüm aracı da yalan söyleyebilir |
| [9](#9-kesişen-ilgiler-tek-yerde-çözülür--ama-hangi-tek-yer) | Hangi "tek yer" | Kod / sidecar / interceptor — üçü de doğru, farklı sorularda |
| [10](#10-sıra-çoğu-zaman-ayardan-daha-önemli) | Sıra | Çalışmama sebebi sık sık bir değer değil, bir sıra |
| [11](#11-teşhis-hipotezle-değil-tek-değişken-değiştirerek) | Teşhis yöntemi | Katmanlı sistemde belirti, sebebin katmanında görünmez |
| [12](#12-öğrenme-sırası-önce-mekanizmayı-yaz-sonra-hazırını-tanı) | Öğrenme sırası | Bir kez elle yazmak, on kez kullanmaktan çok öğretir |

---

## 1. Söylediğine güvenme, kanıtı iste

Aynı ilke üç farklı katmanda, üç farklı fazda çıktı:

| Nerede | Soru | Kanıtı veren |
| --- | --- | --- |
| Faz 3 — fencing token | "kilidi hâlâ ben mi tutuyorum?" | Raft grubu |
| Faz 6 — mesh mTLS | "sen gerçekten hub musun?" | linkerd-identity |
| Faz 6 — Vault | "sana sır verebilir miyim?" | kube-apiserver |

Üçünün ortak yapısı şu: **iddiayı iddia edenin kendisi doğrulamaz.**
Dağıtık kilitte "ben kilidi aldım" diyen istemciye değil, korunan
kaynağın gördüğü fencing token'a bakılır. mTLS'te "ben hub'ım" diyen
bağlantıya değil, sertifikayı imzalayan kimlik otoritesine. Vault'ta
"ben yetkili Pod'um" diyen sürece değil, o Pod'un ServiceAccount
token'ını imzalayan API sunucusuna.

Faz 3'te fencing token yazarken bunun bir kilitleme ayrıntısı olduğunu
sanıyordum. Faz 6'da SPIFFE kimliğini görünce aynı desen olduğu ortaya
çıktı; sır sıfırı problemiyle üçüncü kez karşılaşınca artık bir ilke
haline geldi.

**Pratik karşılığı:** bir güvenlik ya da tutarlılık mekanizması
tasarlarken sorulacak soru "kim ne iddia ediyor" değil, **"bu iddiayı
kim çürütebilir"**dir.

---

## 2. Panoda doğru görünmek, çalışmak değildir

Bu dersi bir kez öğrenmedim — **beş kez** düştüm, her seferinde farklı
bir kılıkta. Hepsinin ortak yanı, kontrolün "yeşil" görünmesiydi.

| Ne | Nasıl göründü | Gerçekte |
| --- | --- | --- |
| Enjekte edilmemiş Pod'lar (Faz 6) | Pod `Running`, her şey yeşil | mTLS **yok**, meshsiz açılmışlar |
| Arena PDB'si (Faz 6) | `ALLOWED DISRUPTIONS: 0` | CRD'de `scale` yok → PDB **hiç çalışmıyor** |
| Ölçüm aracım (Faz 6) | "290 hata, kesinti var" | Kendi hız sınırlayıcımıza çarpmış |
| `/debug/traces` (Faz 7) | Yorumda "politikayla korunuyor" | Yakalayıcı rotanın altında **açık** |
| Zarif kapanış (Faz 6) | Kod özenle yazılmış | SIGTERM dinlenmiyor, **hiç çalışmıyor** |

İkinci satır en sinsisiydi: `ALLOWED DISRUPTIONS: 0` "tam korunuyor"
gibi okunur. Oysa 0, koruma değil **bilinmezlik**ti — disruption
denetleyicisi beklenen Pod sayısını hesaplayamıyordu ve bunu yalnız
API'nin kendi olay kaydı söylüyordu.

**Pratik karşılığı:** bir kontrolün çalıştığını, onu **kırmayı
deneyerek** doğrulayın. Rogue Job yazmak, hata bütçesini bilerek
yakmak, politikayı silip isteğin gerçekten reddedildiğini görmek —
bunlar paranoya değil, minimum düzeyde dürüstlük.

Bunun uzantısı: **yazılı ama hiç ateşlenmemiş bir alarm kuralı, test
edilmemiş kod kadar güvenilmezdir.**

---

## 3. Süreç içi durum, süreçten uzun yaşayan durumla buluşunca patlar

İki ayrı yerde, iki ayrı fazda, **aynı sınıf hata**:

```go
// Faz 6 — player kimlik sayacı
s.nextID++; id := fmt.Sprintf("p-%d", s.nextID)
// İki kopya → ikisi de "p-1" → iki oyuncu aynı kimlik

// Faz 6 — maç kimliği sayacı
mt.id = fmt.Sprintf("m%d", m.nextID.Add(1))
// Yeniden başlatma → yine "m1" → kümede duran eski "arena-m1"e çarpar
```

Birincisinde iki kopya aynı anda koşuyordu; ikincisinde tek kopya
yeniden başlıyordu. Belirtileri de farklıydı: ilki **hiçbir yerde hata
üretmedi** (token imzası geçerli, gateway kabul etti, envanter iki
oyuncunun eşyalarını birleştirdi — sessiz veri bozulması); ikincisi 30
saniye sonra sessizce zaman aşımına düştü.

**En önemli kısmı:** ikisi de yalnızca **ölçek ya da kaos altında**
görünür oldu. Birim testleri, tek süreç e2e testleri, hatta kümedeki
normal akış — hiçbiri yakalamadı. İlkini "2 kopyaya çıkaralım" derken
fark ettim, ikincisini chaos deneyi buldu.

**Pratik karşılığı:** "bu servis durumsuz mu?" sorusunun cevabı
"veritabanı var mı?" değildir. Doğru soru: **"iki kopya aynı anda
koşarsa hangi cevap değişir?"** Sayaçlar, rastgele tohumlar, yerel
önbellekler ve zaman damgaları da durumdur.

---

## 4. Sınır durumları tasarımın parçasıdır, sonradan eklenen yama değil

Faz 0'da lock-free ring buffer'ın `cap=1` durumunda kilitlendiğini
buldum: "dolu" ve "boşaldı" sequence değerleri çakışıyordu. Çözüm
kapasiteyi en az 2'ye yuvarlamak oldu — ama asıl ders, o hatanın
**algoritmanın kendisinden** doğmasıydı, uygulama hatasından değil.

Aynı biçim tekrar etti:

- **Raft §5.4.2** (Faz 0): yeni lider, önceki dönemin girdilerini
  doğrudan commit edemez. Çözüm liderliğe geçince bir **no-op** girdi
  eklemek. Bunu bilmeden yazarsanız kod aylarca çalışır, sonra bir
  bölünmede kilit kaybolur.
- **Aktör kapanış sırası** (Faz 0): `dead → drain → close → drain`.
  Yanlış sıra, testin bazen geçip bazen kalmasına yol açıyordu.
- **LSM sıralaması** (Faz 0): WAL→memtable, fsync→manifest,
  manifest→silme. Her ok bir çökme senaryosunun cevabı.
- **Native sidecar** (Faz 6): kısa ömürlü Pod + klasik sidecar =
  Pod hiç bitmez. Mesh doğru çalışırken bizim yaşam döngümüzü bozuyordu.

**Pratik karşılığı:** bir eşzamanlılık ya da uzlaşma algoritması
yazarken sınır durumu (boş, tek eleman, tam kapasite, terim değişimi,
eşzamanlı kapanış) **önce** düşünülür. Sonradan bulunduğunda, genelde
üretimde bulunur.

---

## 5. Türetilebilen türetilir, türetilemeyen persist edilir

Event sourcing'i (Faz 2) yazarken netleşen ayrım, sonraki her fazda
karar verdirdi:

- **Event log** birincil gerçektir; read model'ler ondan **türetilir**
  ve istenildiği zaman silinip yeniden kurulabilir.
- **Raft log** birincildir; state machine ondan türetilir.
- **Git** birincildir (Faz 6, GitOps); küme durumu ondan türetilir.
- **Arena maç durumu** ise bilerek **hiçbir yere** yazılmaz: 30Hz'lik
  bir döngüye disk yazması eklemek, arenanın var oluş sebebine
  (gecikme) aykırı. Arena Pod'u ölürse maç kaybolur — bu bir eksiklik
  değil, ödenmiş bir bedel.

Son madde önemli: **her şeyi kalıcı yapmak da bir hatadır.** Soru "bu
veriyi saklayabilir miyim" değil, "kaybolursa ne olur ve saklamanın
maliyeti nedir".

---

## 6. Aynı sistemde iki farklı tutarlılık/gecikme profili yaşayabilir

Projenin ana tezi buydu ve Faz 5'te somutlaştı:

| | Hub | Arena |
| --- | --- | --- |
| Öncelik | tutarlılık | gecikme |
| Frekans | 20 Hz | 30 Hz |
| Mekanizma | aktör mesajı, kalıcı log | lock-free ring buffer |
| Durum | event-sourced | bellekte, geçici |
| Aşırı yükte | yavaşlar | **komut düşürür** |
| Bölünmede | **donar** (CAP: C) | etkilenmez |

"Hangisi doğru mimari" sorusunun cevabı yok; **hangi iş yükü için**
sorusunun cevabı var. Aynı kümede, aynı kod tabanında ikisi bir arada
yaşadı.

Aynı ayrım transport seçiminde de çıktı (Faz 5, WS vs QUIC): TCP'nin
"hiçbir kare kaybolmasın" garantisi kayıplı ağda p99'u 150 ms'ye
çıkardı; QUIC datagram %10 kare kaybetti ama kuyruk tıkanmadı (p99≈0).
**Güvenilirlik her zaman istenen özellik değildir.**

---

## 7. Bağımlılık arızasında kendini öldürme

Faz 4'te kod yorumlarında defalarca iddia ettiğimiz ilke, Faz 6'da
chaos deneyleriyle **kanıtlandı**:

```
Vault'a erişim kesildi  → 36/36 giriş başarılı, yalnız tazeleme düştü
NATS'e erişim kesildi   → 29/29 giriş başarılı, hub restart 0
```

Bunu mümkün kılan tasarım kararları: `/healthz` bağımlılık yoklamaz
(NATS düştü diye kubelet'in bizi öldürmesi arızayı büyütür), anahtar
tazeleme hatası ölümcül değildir (eski anahtarla devam), devre kesici
bozuk bağımlılığa yüklenmeyi keser.

**Ama bedeli sessizliktir**: Vault günlerce erişilemez olsa servis
çalışmaya devam eder ve kimse fark etmez. Faz 7'de bu boşluk bir
metrik ve alarmla kapatıldı (`KeyRefreshFailing`) — *"servis çalışıyor
ama rotasyon artık yapılamaz durumda"*.

**Pratik karşılığı:** yumuşak bağımlılık (soft dependency) tasarlarken
iki iş birden yapılır: arızayı yut **ve** yuttuğunu görünür kıl.

---

## 8. Gözlem katmanı, gözlediği şeyi değiştirmemeli

Faz 7'nin özeti, ama kökleri daha eskiye gidiyor:

- **Arena tick ölçümü** `Run()` döngüsüne kondu, `Tick()`'in içine
  değil: histogram çağrısı (~30 ns), Faz 5'te ölçülen 39.8 ns'lik tick
  maliyetini ikiye katlayıp benchmark'ı kirletirdi.
- **`pkg/actor` metrics'e bağımlı olmadı**: dead letter sayacı, çekirdeğin
  zaten sunduğu `WithDeadLetterHandler` kancasıyla bağlandı. Faz 0'ın
  "kütüphanesiz çekirdek" kuralı korundu — çekirdek kanca sunar, montaj
  katmanı bağlar.
- **Metrik toplama hatası 500 dönmez**, `ContinueOnError` ile hatayı
  metrik olarak bildirir.
- **Bozuk `traceparent` başlığı isteği reddettirmez** — sessizce yeni
  trace başlatılır.

Aynı ilkenin tersi de doğru: **ölçüm aracı, ölçtüğü sistemi
tanımıyorsa yalan söyler.** Hammer aracının ilk sürümü kendi hız
sınırlayıcımıza çarpıp "290 hata" saydı; 429 arıza değil, Faz 4'te
bilerek yazdığımız korumaydı.

---

## 9. Kesişen ilgiler tek yerde çözülür — ama hangi "tek yer"?

Aynı problem üç farklı katmanda çözüldü ve **doğru katman her seferinde
farklıydı**:

| İlgi | Nerede çözüldü | Neden orada |
| --- | --- | --- |
| Devre kesici, bulkhead | **uygulama kodu** (Faz 4) | İş mantığına bağlı: "istemci hatası devre için başarıdır" |
| Şifreleme, kimlik | **sidecar** (Faz 6) | Uygulamanın bilmesine gerek yok; süreç dışı olması avantaj |
| RED metrikleri | **interceptor** (Faz 7) | Süreç içi olmalı: gRPC durum kodunu okuyabilmek için |

Üçü de "tek yerde çöz" ilkesini uyguluyor ama farklı yerlerde. Seçimi
belirleyen soru: **bu ilgi, uygulamanın iç anlamına ne kadar
bağımlı?** Hiç bağımlı değilse dışarı çıkabilir (mesh); durum kodu gibi
protokol ayrıntısı gerekiyorsa süreç içinde kalmalı (interceptor); iş
kuralı gerekiyorsa kodun içinde (devre kesici).

---

## 10. Sıra, çoğu zaman ayardan daha önemli

Bir kurulumun çalışıp çalışmaması sık sık bir yapılandırma değerine
değil, **işlemlerin sırasına** bağlı çıktı:

- CRD, onu kullanan iş yükünden **önce** (yoksa "no matches for kind").
- Politikalar, `deny` altındaki iş yüklerinden **önce**.
- `slog.SetDefault`, ilk log satırından **önce** (yoksa başlangıç
  logları yapılandırılmamış çıkar).
- LSM'de WAL→memtable, fsync→manifest, manifest→silme.
- Aktör kapanışında `dead → drain → close → drain`.

GitOps'un sync wave'leri (Faz 6) bu dersi **beyan** haline getirdi:
betik doğru sırayı bir kez uygular, sync wave her uzlaştırmada uygular.

---

## 11. Teşhis: hipotezle değil, tek değişken değiştirerek

Faz 6'daki `appProtocol` arızası bu dersin en pahalı örneğiydi.
Belirtiler üç ayrı katmanda üç ayrı yanlış yeri işaret ediyordu:

| Nerede | Ne görünüyordu | Neyi düşündürüyordu |
| --- | --- | --- |
| Uygulama | `error reading server preface: EOF` | TLS bozuk |
| Proxy | `Connection denied` | Yetkilendirme yanlış |
| `diagnostics policy` | "yetkili" | Politika doğru?! |

İlk hipotezim "politika sırası" oldu ve bir kez **doğrulanmış gibi**
bile oldu (bir Pod yeniden başlatılınca çalıştı). Yanlıştı — kümeyi
sıfırdan kurunca arıza tekrar etti. Gerçek neden tek değişkenli deneyle
bulundu: `appProtocol` kaldır → çalışıyor, `grpc` koy → bozuluyor,
`kubernetes.io/h2c` → kalıcı çözüm.

**Ders teknikten çok yöntemsel:** mesh, hata mesajlarının katmanını
kaydırır. Katmanlı bir sistemde belirti, sebebin bulunduğu katmanda
görünmeyebilir. O yüzden teşhis hipotez doğrulamayla değil, **bir
seferde bir değişken** değiştirilerek yapılır.

---

## 12. Öğrenme sırası: önce mekanizmayı yaz, sonra hazırını tanı

Projenin en başındaki kural ("Faz 0 bileşenleri sıfırdan, kütüphanesiz")
başta yalnızca bir zorluk gibi görünüyordu. Getirisi Faz 5 ve 6'da
ortaya çıktı:

- Kendi **reconcile döngümüzü** yazmış olmak (Faz 5 operator), ArgoCD'yi
  (Faz 6) sihirli bir dağıtım aracı değil **tanıdığımız desenin bir üst
  halkası** olarak görmemizi sağladı: arzu edilen durum, gerçek durum,
  fark kapatma, drift düzeltme, finalizer.
- Kendi **kiralık kilidimizi** yazmış olmak (Faz 3 dlock), Kubernetes
  lider seçimini (Faz 6) "Lease kaynağı = kiralık kilit, arkasında bizim
  Raft'ımız yerine etcd" diye okumamızı sağladı.
- Kendi **W3C trace context**'imizi yazmak (Faz 7), OpenTelemetry SDK'sını
  öğrenmek değil, **bağlam yayılımının nerede kopabileceğini** görmekti.

**Pratik karşılığı:** bir soyutlamayı bir kez elle yazmak, onu on kez
kullanmaktan daha çok öğretiyor. Ama tersi de doğru — üretimde hazırını
kullanmak doğru karar; amaç kütüphaneyi reddetmek değil, **içinde ne
olduğunu bilerek** kullanmak.

---

## Rakamlarla

| | |
| --- | --- |
| Faz | 8 (`faz0` … `faz7`) |
| Go dosyası / test dosyası | 127 / 46 |
| Satır (Go) | ~21.000 |
| Sıfırdan yazılan altyapı | aktör sistemi, lock-free ring buffer, LSM-tree + WAL, Raft, vector clock, CRDT, consistent hashing, dağıtık kilit, event store, JWT, W3C trace context, Vault istemcisi |
| Kavram notu | 10 (`docs/`) |
| Kaos deneyi | 6 |

## Ölçülen değerler

```
ring buffer (padded / unpadded / channel)   173 / 215 / 1022 ns/op
false sharing etkisi (8 çekirdek)           16.05 → 1.25 ns/op  (12.8×)
arena tick                                  39.8 ns
hub world tick p95 (kümede)                 47.5 µs   (bütçe 50 ms)
giriş p95 / p99 (kümede)                    8.0 / 9.6 ms
gRPC sunucu p95 vs istemci p95              475 µs vs 7750 µs
tek trace: istemci vs sunucu span           8.958 ms vs 0.031 ms
WS vs QUIC datagram p99 (%10 kayıpta)       150 ms vs ≈0
kesintisiz dağıtım (player / hub)           0 hata / ~4 sn
```

---

## Bilinçli olarak yapılmayanlar

Bunlar eksik değil, **kapsam dışı bırakılmış** kararlar:

- **Hub yatay ölçek**: bölge aktörleri ve Raft grupları tek süreçte.
  Kesintisiz dağıtımın önündeki tek yapısal engel bu ve dürüstçe
  ölçüldü (~4 sn kesinti), gizlenmedi.
- **Vault üretim modu**: dev modunda (bellekte depolama, sabit root
  token). Sınırı yaşayarak da doğrulandı — Pod yeniden başlayınca
  sırlar gitti.
- **Pushgateway**: arena Pod'ları 90 sn yaşıyor, 15 sn'lik scrape
  aralığıyla eksik örnekleme kaçınılmaz. Alternatifler ve karar
  gerekçesi `docs/observability.md §5`'te.
- **Log toplayıcı, izleme arka ucu, Alertmanager**: sinyaller
  üretiliyor ve doğru biçimde (JSON + `trace_id`, W3C span, `severity`
  etiketleri) ama toplanmıyor.
- **ArgoCD gerçek senkronizasyonu**: manifestler server-side dry-run ile
  doğrulandı; depoda git remote olmadığı için tam sync denenmedi.

Her biri ilgili dokümanda "yapılmadı" olarak yazılı. Bu listenin
kendisi de bir ders: **bir projenin dürüstlüğü, yaptıklarından çok
yapmadıklarını nasıl yazdığıyla ölçülür.**
