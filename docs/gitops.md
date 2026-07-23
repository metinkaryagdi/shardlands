# GitOps — kendi operator'ümüzün bir üst katmanı

Faz 6'nın dördüncü adımının yarısı ([diğer yarısı: kesintisiz
dağıtım](zero-downtime.md)).

## 1. Push CI/CD ile Pull GitOps arasındaki fark

Şu ana kadar dağıtım şöyle yapıldı:

```
geliştirici -> kubectl apply -> küme
```

Bu **push** modelidir. Yaygın ve çalışır ama üç problemi var:

1. **Kimlik bilgisi dışarıda.** Dağıtımı yapan (CI koşucusu ya da laptop)
   kümeye yazma yetkisi taşır. Küme kimlik bilgisi kümenin dışında
   dolaşır.
2. **Drift görünmez.** Biri gece yarısı `kubectl scale` yaptıysa küme
   ile Git ayrışır ve bunu kimse fark etmez. Bir sonraki apply'da hangi
   değişikliğin kasıtlı olduğu belirsizdir.
3. **Kısmi uygulama.** Betik yarıda kalırsa küme tanımsız bir ara
   durumda kalır; kimse "olması gereken" ile "olan"ı karşılaştırmaz.

**Pull** modelinde ise kümenin İÇİNDE koşan bir denetleyici (ArgoCD)
Git'i izler ve farkı kendisi kapatır:

```
geliştirici -> git push -> Git   <---- izler ---- ArgoCD (küme içinde)
                                       uygular ---> küme
```

Kimlik bilgisi kümeden çıkmaz. Drift sürekli tespit edilir. Kısmi
uygulama diye bir şey yoktur, çünkü uzlaştırma tek seferlik bir işlem
değil **sürekli bir döngüdür**.

## 2. Bunu zaten yazmıştık

Faz 5'te kendi operator'ümüzü yazarken şu cümleyi kurmuştuk:

> Reconcile'ın sözleşmesi: idempotent ve seviye-tetiklemeli olmalıdır.
> "Şu olay oldu, şunu yap" değil; "arzu edilen durum bu, gerçek durum
> şu, farkı kapat" der.

ArgoCD **tam olarak aynı döngüdür**, bir seviye yukarıda:

| | Bizim operator | ArgoCD |
| --- | --- | --- |
| Arzu edilen durum | `Arena` CRD'si (etcd'de) | Git deposu |
| Gerçek durum | Pod'lar | Küme kaynakları |
| Uzlaştırma | `Reconcile()` | sync |
| Sürüklenme düzeltme | `Owns(&Pod{})` tetikler | `selfHeal: true` |
| Sahiplenme/temizlik | `ownerReferences` + finalizer | `prune: true` + finalizer |

Faz 5'te operator'ü elle yazmış olmanın kazancı burada ortaya çıkıyor:
ArgoCD sihirli bir dağıtım aracı değil, tanıdığımız desenin kümedeki
en dış halkası. Öğrenme sırası doğruydu — önce mekanizmayı yazdık,
sonra hazırını tanıdık.

## 3. Sync wave: sıra kuralları artık beyan

Faz 6 boyunca iki sıra kuralı acı çekerek öğrenildi:

- CRD, onu kullanan iş yükünden önce (yoksa "no matches for kind Arena").
- Politikalar, `deny` altındaki iş yüklerinden önce.

Bunlar `up.sh` içinde **komut sırası** olarak kodlanmıştı. Sync wave ile
**beyan** hale geliyor:

```
-2  CRD
-1  mesh politikaları
 0  iş yükleri
```

Fark küçük görünüyor ama önemli: betik doğru sırayı **bir kez**
uygular; sync wave **her uzlaştırmada** uygular. Küme yeniden kurulsa,
bir kaynak silinse, drift olsa bile sıra korunur.

## 4. prune ve selfHeal: iddianın iki yarısı

"Git tek gerçek kaynaktır" cümlesi ancak ikisi birden açıkken doğrudur:

- **`prune: true`** — Git'ten silinen kaynak kümeden de silinir. Yoksa
  küme yalnızca EKLEMELERİ takip eder ve zamanla çöp birikir.
- **`selfHeal: true`** — elle yapılan değişiklik geri alınır.

İkincisi bazen can sıkıcıdır ve olması gereken budur: gece yarısı
`kubectl scale` ile yapılan acil müdahale birkaç dakika sonra
kendiliğinden geri alınır. Mesaj net — **müdahale Git'e yazılmalıdır.**
Acil durumda bunu istemezsen `selfHeal`'i kapatmak da bir karardır ama
o zaman "Git tek gerçek kaynak" demeyi bırakman gerekir.

## 5. App of apps: elle yapılan son iş

Önyükleme sorunu şu: ArgoCD Application'larını kim uygular?

Cevap, tek bir kök Application'ı elle uygulamak; o da diğerlerini
çeker. `deploy/gitops/root.yaml` bu proje için "kümeye insan elinin
değdiği son yer"dir.

```bash
kubectl apply -f deploy/gitops/root.yaml
```

## 6. Bu kurulumun dürüst sınırları

- **Depo herkese açık olmalı ya da kimlik bilgisi tanımlanmalı.**
  Manifestlerdeki `repoURL` genel bir GitHub adresi; özel depo için
  ArgoCD'ye bir Secret ile kimlik tanıtmak gerekir. Bu proje için o
  adım atlanmadı, **gerekmedi** — depo genel.
- **İmaj etiketi `:dev` sabit.** Gerçek GitOps'ta yeni sürüm, Git'e
  yazılan yeni bir imaj etiketidir; `:dev` etiketini yerinde
  güncellemek GitOps'un denetlenebilirlik iddiasını zayıflatır (hangi
  commit hangi imajla koştu?). Yerel kind kurulumunda imajlar `kind
  load` ile geldiği için böyle bırakıldı; üretimde etiket, commit SHA
  olurdu.
- **Secret'lar hâlâ düz metin.** Vault adımı (Faz 6 #28) bunu
  değiştirecek. GitOps ile düz metin Secret'ın birlikteliği özellikle
  kötüdür: sır artık yalnız kümede değil, **Git geçmişinde** de durur.
  Sealed Secrets / SOPS / External Secrets bu boşluğu kapatan yaygın
  çözümler.
- **ArgoCD kendisi GitOps ile yönetilmiyor.** Kurulumu bir betik
  yapıyor. "Kim denetleyiciyi denetler" sorusunun standart cevabı ArgoCD'yi
  kendi kendine yönetir hale getirmektir; bu kurulumda yapılmadı.
