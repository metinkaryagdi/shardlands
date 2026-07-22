# WebSocket (TCP) vs QUIC: Gecikme ve Head-of-Line Blocking

Deney kodu: [experiments/transport](../experiments/transport). Çalıştırma:

```bash
go test -v -run TestLossy ./experiments/transport/
```

## Soru

Shardlands'te hub 20Hz, arena 30Hz snapshot yayınlıyor. Taşıma katmanı
şu an WebSocket (TCP). **Kayıplı bir ağda bu seçim ne kaybettiriyor?**

## Mekanizma: neden TCP oyun için ters?

TCP **tek, sıralı, güvenilir bir bayt akışıdır**. Bir segment kaybolursa
alıcı, o segment yeniden iletilene kadar **arkasından gelen ve zaten
ulaşmış** veriyi uygulamaya teslim EDEMEZ — sıralama garantisi bunu
zorunlu kılar. Buna **head-of-line (HOL) blocking** denir.

Oyun akışında bu tam ters bir davranıştır: kare N+1 elimizdeyken kare
N'i beklemenin hiçbir değeri yoktur, çünkü **yeni kare eskisini zaten
geçersiz kılar**. TCP bize istemediğimiz bir garantiyi (hiçbir kare
kaybolmasın) istemediğimiz bir bedelle (hepsi sırayla beklesin) satar.

QUIC iki çıkış sunar:
1. **Bağımsız stream'ler** — bir stream'deki kayıp diğerlerini bloklamaz.
2. **Güvenilmez datagram'lar** (RFC 9221) — kayıp kare kaybolur, yeniden
   iletilmez, kuyruk tıkanmaz. Anlık durum yayını için doğru araç.

Bu deneyde ikincisini ölçüyoruz.

## Düzenek

- Sunucu, 15ms aralıkla 60 "kare" gönderir; her karenin içinde gönderim
  zaman damgası vardır (iki uç aynı süreçte, saat ortak).
- Araya bir proxy konur ve **her 8. birim bozulur**.
- İstemci her karenin gecikmesini ölçer; p50/p95/p99/max raporlanır.

**Dürüstlük notu — iki taşıma farklı biçimde bozulur:**

| | Nasıl bozuluyor | Neden böyle |
|---|---|---|
| QUIC/UDP | Paket **gerçekten düşürülür** | UDP'de kayıp doğal olarak budur |
| TCP | Parça **geciktirilir** (150ms) | Bayt düşürmek akışı bozardı (TCP uçtan uca güvenilir); gecikme, yeniden iletim beklemesinin *gözlemlenebilir sonucunu* birebir üretir: sıralı teslim yüzünden arkadaki her şey bekler |

## Sonuçlar (i7-13650HX, loopback)

**Sağlıklı ağ (temel çizgi):**

```
websocket(TCP)   gönderilen=40 alınan=40 kayıp=0.0%  p50=0s p95=0s p99=0s max=0s
quic(datagram)   gönderilen=40 alınan=40 kayıp=0.0%  p50=0s p95=0s p99=0s max=453µs
```

Fark yok — ikisi de her kareyi anında teslim ediyor. **Fark taşımadan
değil, kayıptan doğuyor.**

**Kayıplı ağ (her 8. birim bozulur):**

```
websocket(kayıplı) gönderilen=60 alınan=60 kayıp=0.0%   p50=15.5ms p95=150.0ms p99=150.1ms max=150.3ms
quic(kayıplı)      gönderilen=60 alınan=54 kayıp=10.0%  p50=0s     p95=0s      p99=0s      max=0s
```

Tablo hâlinde:

| | Teslim edilen kare | p50 | p99 | max |
|---|---|---|---|---|
| WebSocket (TCP) | **60/60** (kayıpsız) | 15.5 ms | **150 ms** | 150 ms |
| QUIC (datagram) | 54/60 (%10 kayıp) | **0 s** | **0 s** | 0 s |

## Yorum

- **TCP hiçbir kareyi kaybetmedi** — ama p99 gecikmesi enjekte edilen
  duraklamanın tamamını yedi (150ms). Üstelik p50'nin bile 15.5ms olması
  şunu gösteriyor: bir duraklama yalnız kendi karesini değil, **arkasında
  biriken kareleri de** geciktiriyor. HOL blocking'in imzası budur.
- **QUIC datagram %10 kare kaybetti** — ama kalan karelerin hepsi
  zamanında geldi (p99 ≈ 0). Kayıp kare için kimse beklemedi.
- Oyun için doğru takas ikincisidir: 90ms geç gelen bir pozisyon
  güncellemesi **zaten yanlıştır**; hiç gelmemesi daha iyidir çünkü
  sıradaki kare doğruyu getirir.

## Peki neden hâlâ WebSocket kullanıyoruz?

Ölçüm QUIC'i haklı çıkarıyor ama geçiş bedava değil:

- **Tarayıcı erişimi.** Tarayıcıdan ham QUIC datagram'ı kullanmak
  WebTransport gerektirir; destek WebSocket kadar yaygın ve olgun değil.
- **Altyapı.** UDP birçok kurumsal ağda/proxy'de engelli; WebSocket
  HTTP'nin açtığı yoldan geçer.
- **Karmaşıklık.** TLS zorunlu, sertifika yönetimi, MTU/GSO ayarları,
  bağlantı geçişi (connection migration) gibi yeni kavramlar.
- **Bizim yükümüz henüz bunu gerektirmiyor:** tek makinede loopback'te
  fark sıfır; sorun ancak gerçek kayıplı ağlarda görünür.

**Karar:** WebSocket varsayılan kalıyor; QUIC/WebTransport, arena
taşıması için *opsiyonel* bir yol olarak Faz 6+'da değerlendirilecek.
Bu deney, o kararın gerekçesini ölçümle kayıt altına alıyor — "hissiyat"
yerine sayı.

## Ders

- **Güvenilirlik her zaman istenen bir özellik değildir.** TCP'nin
  garantisi, anlık durum yayınında bir maliyete dönüşür. Doğru soru
  "hangi taşıma daha iyi" değil, *"bu veri için hangi garantiyi
  istiyorum"*dur — Faz 2'deki "her şey event olmaz", Faz 3'teki "bölge
  CP, sayaç AP" ayrımlarının taşıma katmanındaki karşılığı.
- **p50'ye bakmak yanıltır.** Kayıplı senaryoda ortalama makul görünür;
  hikâyeyi p99 ve max anlatır.
