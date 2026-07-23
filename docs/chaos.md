# Chaos Engineering — hipotezi önce yaz

Faz 6'nın son adımı.

## 1. Chaos engineering "rastgele kırmak" değildir

Yaygın yanlış anlama: "üretimde maymun salıp ne olacağına bakmak".
Gerçekte bu bir **deney yöntemidir** ve sırası bellidir:

1. **Kararlı durum hipotezi (steady state).** Sistem "çalışıyor"
   derken neyi kastediyoruz? ÖLÇÜLEBİLİR bir cümle olmalı: "saniyede
   bir giriş isteği 200 döner", "arena kareleri 30Hz akar".
2. **Fay enjeksiyonu.** Tek bir şeyi boz.
3. **Hipotezi doğrula.** Kararlı durum korundu mu? Korunmadıysa ne
   kadar sürede geri geldi?
4. **Patlama yarıçapını genişlet.** Küçük başla, güven kazandıkça büyüt.

Kritik olan 1. adım. Hipotez yazmadan fay enjekte etmek "log izlemek"
tir; bir şeyin bozulduğunu görürsün ama neyin bozulmadığını
söyleyemezsin. Bu projede kararlı durum ölçütü `internal/smoke`
araçlarıdır — ve o araçların kendisi de bir kez yanlış ölçtü
([zero-downtime.md §8](zero-downtime.md): hız sınırlayıcıyı kesinti
sandı).

## 2. Neden küme seviyesinde? Zaten test etmiştik

Faz 0'dan beri arıza testleri var: `pkg/raft` bölünme testleri,
Faz 3'ün CAP/PACELC deneyi, `pkg/resilience` devre kesici testleri.
Hepsi süreç içinde, sahte taşıyıcılarla.

Kümede farklı olan şey **süreç içinde simüle edilemeyen katmanlardır**:
kubelet, scheduler, CNI, gerçek TCP, PDB, eviction API. Faz 6 boyunca
bulunan hataların hiçbiri birim testinde çıkmazdı — `appProtocol`
yanlışı, sessizce meshsiz açılan Pod'lar, dinlenmeyen SIGTERM. Chaos
deneyleri o katmanı hedefler.

## 3. Neden Chaos Mesh yok

Chaos Mesh (ya da LitmusChaos) bu iş için standart araçtır ve ağ
gecikmesi/paket kaybı enjeksiyonunda elle yapılamayacak şeyler sunar.
Bu kurulumda kullanılmadı, sebebi dürüstçe:

- Kümenin üstünde zaten Linkerd + ArgoCD CRD'leri + Vault var; makine
  bu oturumda bir kez zaten Docker'ı düşürecek kadar zorlandı.
- İhtiyacımız olan fayların çoğu Kubernetes ilkelleriyle üretilebiliyor:
  Pod silme, `drain`, kopya sayısını sıfırlama.
- **Ağ bölünmesi için elimizde zaten bir araç var**: zero-trust
  politikaları. Bir `AuthorizationPolicy`'yi silmek, mesh proxy'sinin o
  bağlantıyı anında reddetmesi demektir — gerçek, bağlantı düzeyinde ve
  tek komutla geri alınabilir bir bölünme.

Son madde bu fazın hoş sürprizi: **güvenlik katmanı, kaos aracı olarak
da çalışıyor.** Politikayı silmek "bu iki servis birbirini göremiyor"
durumunu birebir üretiyor.

Eksik kalan: gecikme ve paket kaybı enjeksiyonu. Bunlar için gerçekten
Chaos Mesh gerekir ve bu kurulumda yapılmadı.

## 4. Deneyler

Her deney `deploy/chaos/` altında ayrı bir betik. Ortak kalıp:
hipotezi yaz → ölç → boz → ölç → hükmü ver.

| # | Fay | Hipotez |
| --- | --- | --- |
| 1 | player Pod'u öldür | Giriş yolu HİÇ kesilmez (2 kopya, PDB, preStop) |
| 2 | hub Pod'u öldür | Kısa kesinti olur; event store bozulmaz, sistem kendi kendine döner |
| 3 | Vault'a erişimi kes | Girişler ÇALIŞMAYA DEVAM EDER (önbellekteki anahtar), yalnız tazeleme başarısız olur |
| 4 | NATS'e erişimi kes | Hub ayakta kalır; read model'ler tazelenmez ama süreç ölmez |
| 5 | Arena Pod'unu maç ortasında öldür | Operator yeniden yaratır; maç durumu KAYBOLUR (bellekte) |
| 6 | Düğüm boşalt (`drain`) | PDB'ler saygı görür: player'da en az 1 kopya kalır, arena tahliye edilmez |

3 ve 4'ün hipotezi Faz 4'te yazdığımız bir iddianın sınanmasıdır:
**bağımlılık arızasında kendini öldürme.** Kod yorumlarında bunu
defalarca iddia ettik; burada kanıtlanıyor ya da çürütülüyor.

## 5. Sonuçlar

Ölçülen değerler ve çıkan sürprizler [deploy/README.md](../deploy/README.md)'de.
