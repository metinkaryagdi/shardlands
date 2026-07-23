# Kesintisiz Dağıtım — ve nerede mümkün olmadığı

Faz 6'nın dördüncü adımının yarısı ([diğer yarısı: GitOps](gitops.md)).

Bu notun ana iddiası şu: **kesintisiz dağıtım bir ayar değil, bir
zincirdir** ve zincir en zayıf halkası kadar sağlamdır. Halkaların
hepsini tek tek gözden geçirip hangisinin bizde gerçekten kurulu
olduğunu, hangisinin kurulamayacağını yazıyorum.

## 1. Zincirin halkaları

| # | Halka | Bizde |
| --- | --- | --- |
| 1 | Birden çok kopya | player ✓, operator ✓ (sıcak yedek), hub ✗ |
| 2 | `maxUnavailable: 0` | ✓ |
| 3 | Readiness probu doğru cevap versin | ✓ (`/readyz`) |
| 4 | SIGTERM'i dinle ve zarif kapan | ✓ (eksikti, eklendi) |
| 5 | preStop gecikmesi | ✓ |
| 6 | PodDisruptionBudget | player ✓, arena ✓, hub ✗ (imkânsız) |
| 7 | İstemci yeniden bağlansın | ✓ (eksikti, eklendi) |

4 ve 7 bu adımda eklendi; ikisi de "vardır sanılan" ama olmayan
şeylerdi. Aşağıda neden kritik olduklarını yazıyorum.

## 2. Ölçeklenebilirlik: "durumsuz" sandığın servisin gizli durumu

Player servisini 2 kopyaya çıkarmak tek satırlık bir manifest
değişikliği gibi görünüyordu. Değildi.

```go
s.nextID++
id := fmt.Sprintf("p-%d", s.nextID)
```

İki kopya da "p-1" basar. İki farklı oyuncu aynı kimliği taşır ve
**hiçbir yerde hata oluşmaz** — token imzası geçerlidir, gateway kabul
eder, envanter read model'i iki oyuncunun eşyalarını birleştirir.
Sessiz veri bozulması, en kötü arıza türü.

Servisin veritabanı yoktu, disk yazmıyordu, PVC istemiyordu. Yine de
durumluydu. **Durum, kalıcılık demek değildir**: sayaçlar, rastgele
tohumlar, yerel önbellekler, biriken zaman damgaları — hepsi durumdur.
Bir servisin ölçeklenip ölçeklenemeyeceğini "veritabanı var mı" diye
bakarak anlayamazsın; "iki kopya aynı anda koşarsa hangi cevap değişir"
diye sorarak anlarsın.

Çözüm kimliğe kopya ön eki eklemek oldu (`p-kprj2-1`), ön ek de aşağı
yönlü API'den gelen Pod adından türüyor. Alternatif UUID'ydi; okunabilir
kimlikler hata ayıklamada değerli olduğu için ön ek tercih edildi.

## 3. SIGTERM ve preStop: kapanışın iki ayrı yarışı

### SIGTERM dinlenmiyordu

`signal.Notify(sig, os.Interrupt)` yazılıydı. Kubernetes kapanışı
**SIGTERM** ile ister, SIGINT ile değil. Yani hub, `Stop()` içindeki
`Drain()` ve zarif kapanış koduna **hiç ulaşmıyordu**: grace period
boyunca hiçbir şey yapmadan bekliyor, sonunda SIGKILL yiyordu.

Kodda güzelce yazılmış bir kapanış yolu vardı ve kümede hiç
çalışmıyordu. Bu, "yerelde test ettim" ile "kümede çalışıyor" arasındaki
farkın tipik örneği: `Ctrl+C` SIGINT yollar, kubelet SIGTERM.

### Endpoints yayılması SIGTERM'den yavaştır

Pod silinince iki süreç **paralel** başlar:

```
kubelet ──SIGTERM──> konteyner            (hızlı, yerel)
apiserver ──> endpoints denetleyicisi ──> kube-proxy / mesh proxy'leri
                                          (yavaş, dağıtık)
```

Uygulama SIGTERM'i alıp hemen kapanırsa, hâlâ o Pod'a yönlendiren
istemciler `connection refused` alır. Kapanış "zarif"tir ve istek yine
de kaybedilir — çünkü sorun uygulamanın nasıl kapandığı değil, **ne
zaman** kapandığıdır.

`preStop.sleep: 5` SIGTERM'i geciktirir: yayılma tamamlanır, sonra
kapanış başlar. Uygulamada tek satır değişiklik gerektirmez.

Distroless imajlarda bu tarihsel olarak acı vericiydi: `exec: ["sh",
"-c", "sleep 5"]` çalışmaz, kabuk yok. Kubernetes 1.30'dan beri yerleşik
`sleep` eylemi var; bu proje 1.36'da koştuğu için onu kullanıyor.

## 4. Hub neden kesintisiz olamaz — ve bunu neden gizlemiyoruz

Hub tek kopya. Sebebi 20-server.yaml'da yazılı: bölge aktörleri ve shard
Raft grupları tek süreçte yaşıyor, ikinci kopya ikinci bir dünya açardı.

Bu, kesintisiz dağıtımı **yapısal olarak** imkânsız kılar:

- İkinci kopya yok → rolling update kaçınılmaz olarak boşluk bırakır.
- PVC `ReadWriteOnce` → yeni Pod, eskisi diski bırakmadan bağlanamaz.
- PDB hiçbir şey yapamaz: `minAvailable: 1` düğüm boşaltmayı sonsuza
  kadar bloklar, `maxUnavailable: 1` ise hiçbir şey korumaz. **PDB kopya
  sayısı olmayan yerde kullanılamaz.**

Buraya "kesintisiz" yazmak kolay olurdu ve panoda kimse fark etmezdi.
Elimizdeki gerçek hafifletmeler şunlar ve sınırları belli:

1. **Zarif kapanış**: `Drain()` → `/readyz` 503 → yeni oturum gelmez;
   açık WS oturumları grace period boyunca çalışmaya devam eder.
2. **İstemci yeniden bağlanması** (§5).
3. **Kısa kesinti**: tek Pod'un yeniden başlaması ~5-10 saniye.

Gerçek çözüm mimari: bölgeleri süreçlere dağıtmak ve Raft üyelerini ayrı
Pod'lara çıkarmak. Faz 7 sonrasının işi ve bu notun kapsamı değil.

## 5. Son metre istemcidedir

Sunucu tarafında yapılabilecek her şey yapıldıktan sonra, hub yeniden
dağıtıldığında oyuncunun WebSocket'i **yine de** kopar. Geriye kalan tek
çare istemcinin toparlamasıdır.

İstemcide `ws.onclose` yalnız "bağlantı koptu" yazıyordu. Artık üstel
geri çekilme + **jitter** ile yeniden bağlanıyor. Jitter şart: kesintiden
çıkan sunucuya bütün istemciler aynı anda yüklenirse (thundering herd)
sunucu tekrar düşer. Faz 4'teki yeniden deneme tartışmasının istemci
tarafındaki karşılığı.

Dikkat edilen ayrıntı: yeniden bağlanmada **tek seferlik kurulum
tekrarlanmamalı**. İlk `onopen` içinde kurulan `setInterval`'lar her
kopuşta bir daha kurulsaydı, birkaç kesintiden sonra istemci saniyede
onlarca istek atardı — kendi kendine DoS.

## 6. PodDisruptionBudget: en çok yanlış anlaşılan kaynak

PDB, Pod'un **çökmesini engellemez**. OOM, düğüm arızası, kernel panic —
bunlar gönülsüz kesintilerdir ve PDB'nin haberi olmaz.

PDB yalnız **eviction API'sinden** geçen işlemleri durdurur: `kubectl
drain`, otomatik ölçekleyicinin düğüm toplaması, düğüm yükseltmeleri.
Yani "bakım sırasında beni tek başıma bırakma" sözleşmesidir.

Bizdeki iki kullanım zıt uçlarda:

- **player**: `minAvailable: 1` — klasik kullanım, iki kopyadan biri
  hep ayakta.
- **arena**: `maxUnavailable: 0` — hiçbir arena gönüllü tahliye
  edilemez. Sert kural; düğüm boşaltmayı **bloklar**. Kabul edilebilir
  olmasının tek sebebi arenaların TTL'inin 5 dakika olması. Takas açık:
  bir maçı yarıda kesmektense bakımı birkaç dakika geciktiriyoruz. Uzun
  ömürlü bir iş yükünde aynı ayar drain'i hiç bitirmezdi.

## 7. Operator: yatay ölçek değil, sıcak yedek

Operator 2 kopyaya çıkarıldı ama **aynı anda yalnız biri çalışır** —
`--leader-elect` ikincisini beklemeye alır.

Amaç reconcile'ı paralelleştirmek değil (aksine, iki reconciler aynı
Arena için yarışırdı); **devralma süresini kısaltmak**. Lider ölünce
yedek kirayı ~15 saniyede alır; tek kopyada yeni Pod'un çizelgelenip
imaj çekip başlaması çok daha uzun sürerdi.

Kilit bir `Lease` kaynağıdır: Faz 3'te kendi yazdığımız `pkg/dlock`'un
yaptığı işin aynısı — kiralık kilit, süresi dolunca serbest kalır. Fark,
buradaki kilidin arkasında bizim Raft'ımızın değil etcd'nin olması.

## 8. Ölçüm

İddia edilen şey ölçülmelidir. `internal/smoke -hammer` aralıksız giriş
yapar ve başarısızlıkları sayar:

```bash
go run ./internal/smoke -hammer=70s
# başka bir kabukta:
kubectl -n shardlands rollout restart deployment/player
```

Kümede ölçülen: player rolling update'inde **70/70 başarılı, 0 hata**;
hub Pod'u silindiğinde **4 hata** (~4 saniyelik boşluk). Beklenen tablo
bu — §4'te yazılanın sayısal karşılığı.

### Aracın kendi hatası

İlk sürüm 200ms aralıkla vuruyordu. Sonuç: 290 "hata", hepsi 429.
Ölçtüğü şey dağıtım kesintisi değil **kendi hız sınırlayıcımızdı**
(pkg/ratelimit, IP başına 1/sn). Yani araç, sistemin doğru çalışan bir
özelliğini arıza diye raporladı.

İki düzeltme yapıldı: 429 ayrı sayılıyor (yük atma arıza değildir) ve
varsayılan aralık sınırlayıcının doldurma hızına eşit. Ders küçük ama
tekrarlayan cinsten: **bir ölçüm aracı, ölçtüğü sistemin davranışlarını
tanımıyorsa yalan söyler.** Faz 6'da bunun üçüncü örneği bu — ilk ikisi
`appProtocol` teşhisi ve enjektör sessizliğiydi.
