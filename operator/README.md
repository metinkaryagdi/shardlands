# operator — Arena Kubernetes Operator

Arena instance'larını **CRD** olarak deklaratif biçimde sağlar.

## Operator pattern nedir, neden burada?

Kubernetes'in kendi denetleyicileri (Deployment, ReplicaSet…) hep aynı
şeyi yapar: *arzu edilen durumu* (spec) *gerçek durumla* karşılaştırıp
farkı kapatır. Operator bu mantığı **kendi domain nesnene** genişletmendir:
Arena diye bir kaynak tanımlarsın, bir **reconcile döngüsü** onu gerçek
kılar.

Arena neden uyuyor? Talep üzerine doğan, kısa ömürlü, TTL'i olan ve
bitince temizlenmesi gereken bir iş yükü. Alternatif — matchmaking'in
doğrudan Pod yaratıp izlemesi — imperatif ve kırılgandır: süreç çökerse
kim temizleyecek? CRD'de durum **etcd'dedir**; controller yeniden başlar
ve kaldığı yerden devam eder.

## Reconcile sözleşmesi

Döngü **idempotent ve seviye-tetiklemelidir** (level-triggered):
"şu olay oldu, şunu yap" değil, "arzu edilen bu, gerçek şu, farkı kapat".
Aynı Arena için yüz kez çağrılsa sonuç aynıdır — testte de bunu
doğruluyoruz (tekrarlı reconcile ikinci Pod yaratmaz). Kaçan olay,
yeniden başlatma veya sıra bozukluğu bu sayede kendini düzeltir.

Akış:

```
Arena oluşturuldu
  → finalizer eklenir (temizlik garantisi)
  → Pod yaratılır (ownerRef ile: GC bağlantısı), status=Pending, startTime
  → Pod Running    → status=Running + endpoint yayınlanır
  → Pod Succeeded  → status=Completed → Pod temizlenir
  → Pod Failed     → status=Failed
  → TTL aşıldı     → status=Completed ("ttl exceeded") → Pod temizlenir
Arena silindi
  → finalizer: Pod silinir, sonra finalizer kaldırılır
```

**Finalizer neden?** Kaynak silinince Pod'un da gittiğinden emin olmak
için. ownerReference zaten GC yapar, ama finalizer temizliğin
*tamamlandığını* garanti eder ve sıralamayı bize bırakır.

## Sağlayıcı entegrasyonu

`services/matchmaking` iki sağlayıcıyla çalışır ve **saga tarafı ikisini
de bilmez**:

| | LocalProvisioner | K8sProvisioner |
|---|---|---|
| Arena nerede | Bu süreçte (goroutine) | Kümede (Pod) |
| Sağlama | `arena.New` + `Run` | Arena CR yaratır, operator Pod'u açar |
| Handle | `*arena.Arena` | `Endpoint` (IP:port) |
| Temizlik | `Stop()` | CR silinir, finalizer Pod'u toplar |

Faz 5'in başında `Provisioner` arayüzünü koymanın karşılığı bu: operator
eklendiğinde saga'da **tek satır** değişmedi.

## Kurulum (kind)

```bash
kind create cluster --name shardlands
kubectl apply -f operator/config/crd/
go run ./cmd/operator                     # kubeconfig'deki kümeye bağlanır
kubectl get arenas -w
```

Örnek kaynak:

```yaml
apiVersion: shardlands.dev/v1alpha1
kind: Arena
metadata:
  name: arena-demo
spec:
  mode: "1v1"
  ttlSeconds: 120
  players:
    - {id: p1, name: bir, team: 0}
    - {id: p2, name: iki, team: 1}
```

## Test stratejisi

Küme gerektirmez: `controller-runtime`'ın **fake client**'ı ile reconcile
döngüsü doğrudan sınanır (Pod yaratma, durum yansıtma, TTL, finalizer
temizliği, idempotentlik). Saat enjekte edilebilir olduğu için TTL testi
deterministiktir.

## Bilinçli sınırlamalar

- **Uzak arenaya oturum bağlama yok.** K8sProvisioner endpoint döner ama
  gateway şu an yalnız in-process arena handle'ıyla çalışır; oturumu
  uzak bir arena Pod'una yönlendirmek (proxy/redirect) Faz 6'nın konusu.
  `gateway.Assign` bunu açıkça reddeder — sessizce yanlış davranmaz.
- Arena imajı (`shardlands/arena:dev`) bu depoda üretilmiyor; Pod
  şablonu ve env sözleşmesi hazır.
- DeepCopy'ler elle yazıldı (controller-gen bağımlılığı yok).
- Webhook/validation admission yok; şema doğrulaması CRD'nin
  OpenAPI bölümünde.
