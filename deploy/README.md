# Shardlands — Konteynerleştirme ve Kubernetes'e taşıma

Faz 6'nın ikinci adımı: Faz 1'den beri `go run ./cmd/server` ile tek
süreçte koşan sistemi kümeye taşımak. Kod tarafında değişen tek şey iki
ortam değişkeni oldu (`NATS_URL`, `ARENA_NAMESPACE`) — geri kalan her şey
Faz 5'te konan soyutlamaların (`Provisioner`, `bus.Bus`) yerine oturması.

```
deploy/
  docker/       Dockerfile.server | .arena | .operator
  k8s/base/     namespace, NATS, sunucu, operator (+RBAC)
  k8s/local/    yalnız yerel küme için NodePort
  kind/         küme yapılandırması + up/down betikleri
```

## Hızlı başlangıç

Gereken araçlar: Docker, [kind](https://kind.sigs.k8s.io/), `kubectl`.

```bash
./deploy/kind/up.sh
```

Windows:

```powershell
.\deploy\kind\up.ps1
```

Sonra `http://localhost:30080`. Temizlik: `./deploy/kind/down.sh`.

## İmajlar: neden multi-stage + distroless?

Üç Dockerfile da aynı kalıbı izler: `golang:1.26` içinde derle, çıkan
statik binary'yi `gcr.io/distroless/static-debian12:nonroot` üstüne kopyala.

| Karar | Gerekçe |
| --- | --- |
| `CGO_ENABLED=0` | Statik binary. libc'ye bağlı olmayan bir ikili, içinde libc bulunmayan bir imajda koşabilir. |
| distroless | Kabuk, paket yöneticisi, `curl` yok. Ele geçen bir konteynerde saldırganın kullanacağı alet kalmaz — zero trust'ın iş yükü tarafı. |
| `nonroot` | uid 65532. Konteyner kaçışlarının çoğu root ayrıcalığını varsayar. |
| `-trimpath -ldflags="-s -w"` | Yapı yollarını ve sembol tablosunu at: küçük imaj, daha az bilgi sızıntısı. |
| Ayrı `go mod download` katmanı | `go.mod`/`go.sum` değişmedikçe bağımlılık katmanı önbellekten gelir. |

Bedeli dürüstçe: distroless'ta `kubectl exec ... -- sh` yok. Hata ayıklama
`kubectl logs`, metrikler ve gerektiğinde ephemeral debug container ile
yapılır. Bu bilinçli bir takas — üretimde konteynere girip bakmak zaten
istenen bir alışkanlık değil.

## Neden bu iş yükü tipleri?

| Bileşen | Tip | Neden |
| --- | --- | --- |
| `nats` | StatefulSet + PVC | JetStream stream'leri diskte. Sabit kimlik (`nats-0.nats`) DNS'ten adreslenir. |
| `shardlands` (hub) | StatefulSet + PVC | Event store (LSM + WAL) ve shard Raft log'ları diskte. Kimlik ve disk Pod'a bağlı olmalı. |
| `shardlands-operator` | Deployment | Tamamen durumsuz: tüm gerçeği API sunucusundan okur. Çökerse veri değil, yalnız gecikme kaybedilir. |
| arena | Pod (operator yaratır) | Tek maçlık, `RestartPolicy: Never`. Bitince `Succeeded` → operator temizler. |

### Gömülü NATS neden ayrıldı?

Faz 4'te bus, sunucu sürecine gömülüydü (`bus.StartEmbedded`). Tek süreçte
bu doğru karardı — testler ve `go run` için harici bağımlılık gerekmiyordu.
Kümede yanlış olurdu: sunucu Pod'u yeniden başladığında bus da gidiyor,
tüketicilerin akışları sıfırlanıyordu. `server.Config.NATSURL` boşsa gömülü
mod korunur (testler hâlâ tek süreç koşar), doluysa harici NATS'a bağlanır.
Aynı kod, iki topoloji.

### Hub neden tek replika?

Bölge aktörleri ve shard Raft grupları şu an **tek süreçte** yaşıyor.
İkinci bir replika ikinci bir dünya açardı — split-brain. Gerçek yatay
ölçek için bölgelerin süreçlere dağıtılması ve Raft üyelerinin ayrı
Pod'lara çıkması gerekir; bu bilerek Faz 7 sonrasına bırakıldı.
Ölçeklenen kısım zaten arenalar: her maç kendi Pod'unda, kendi düğümünde.

## RBAC: iki farklı dar yetki

Kümede iki bileşen Arena kaynaklarına dokunuyor ve **yetkileri kasten
farklı**:

- **Sunucu** (`Role`, namespace'e sınırlı): `arenas` üzerinde
  create/get/list/watch/delete, `arenas/status` üzerinde yalnız get.
  Pod yaratma yetkisi **yok**.
- **Operator** (`ClusterRole`): `arenas` + `arenas/status` +
  `arenas/finalizers`, ve `pods` üzerinde create/delete.

Bu ayrım mimarinin yetki düzeyindeki yansıması: sunucu *ne istediğini*
söyler (deklaratif kayıt), *nasıl olacağı* operator'ün işidir. Ele geçen
bir hub Pod'u kümede keyfi Pod açamaz.

Operator ayrıca `--namespace=shardlands` ile başlatılır; controller-runtime
cache'i o namespace'e daralır, yani kümenin geri kalanını list/watch etmez.

## Sırlar

`shardlands-secret` şu an düz metin bir `Secret` — JWT imzalama anahtarı.
**Üretime böyle gitmez**: Kubernetes Secret'ları varsayılan olarak yalnız
base64'tür, şifreli değildir. Vault entegrasyonu (Faz 6'nın sonraki adımı)
bunu değiştirecek; buraya kadar bilinçli bir borç olarak duruyor.

## Sık karşılaşılan tuzaklar (yaşanmış)

Aşağıdakilerin hepsi bu kurulumu ilk kez ayağa kaldırırken **gerçekten**
patladı; manifestlerdeki ilgili satırlar bu yüzden var.

- **`container has runAsNonRoot and image has non-numeric user`**:
  Dockerfile'da `USER nonroot` (isim) yazılırsa kubelet kullanıcının root
  olmadığını doğrulayamaz ve Pod hiç başlamaz — imaj gerçekten nonroot
  olsa bile. Çözüm iki taraflı: Dockerfile'da `USER 65532:65532`,
  manifestte `runAsUser: 65532`. En sinsi kısmı, hatanın
  `CreateContainerConfigError` olarak görünmesi: "yapılandırma" hatası
  gibi durur ama sebep güvenlik bağlamıdır.
- **`ImagePullBackOff`**: kind düğümleri ana makinenin imaj deposunu
  görmez. `kind load docker-image` şart; `imagePullPolicy: IfNotPresent`
  da öyle (`Always` olsaydı yerel imaj yok sayılır, Docker Hub'a gidilirdi).
- **PVC'de `permission denied`**: distroless nonroot uid 65532, PVC ise
  root'a ait yaratılır. `fsGroup` olmadan event store ilk yazmada patlar.
  NATS için aynısı uid 1000 ile geçerli.
- **CRD sırası**: `kubectl apply -f deploy/k8s/base/` CRD'den önce
  çalışırsa operator cache senkronizasyonunda "no matches for kind Arena"
  ile ölür. Betik önce CRD'yi kurup `Established` bekliyor.
- **Kontrol düzlemi flep atıyor**: üç düğümlü kind + eşzamanlı `docker
  build` aynı CPU'yu paylaşır; `kube-scheduler`/`kube-controller-manager`
  lease yenileyemeyip CrashLoopBackOff'a girer, Pod'lar `Pending` takılır.
  Arıza manifestlerde değil, makinededir: imaj derlemesi bitince kendi
  kendine düzelir.

## Doğrulama

Uçtan uca duman testi (dışarıdan, gerçek bir istemci gibi):

```bash
go run ./internal/smoke
```

İki oyuncu giriş yapar, ikisi de 1v1 kuyruğuna girer ve arena kareleri
sayılır. Sıfır kare gelirse çıkış kodu 1'dir. Tek süreç kurulumu için
`BASE=http://localhost:8080 go run ./internal/smoke`.

Elle bakmak için:

```bash
kubectl -n shardlands get arenas -w
kubectl -n shardlands get pods -o wide
kubectl -n shardlands logs -f deployment/shardlands-operator
```

Kümede gözlenen tam zincir:

```
oyuncu (WS, hub Pod'u — worker2)
  → matchmaking saga → Arena CRD yazıldı        arena-m1  Pending
  → operator reconcile → Pod açıldı (worker)    arena-m1  Running  10.244.1.6:7777
  → gateway oturumu uzak Pod'a gRPC ile vekil etti → 30Hz "arena" kareleri
  → maç bitti → Pod Succeeded                   arena-m1  Completed
  → operator Pod'u sildi
```

`get pods -o wide` çıktısında arena Pod'unun hub'dan **başka bir
düğümde** koştuğu görülür: gateway'in gRPC vekilliği o noktada gerçek bir
düğümler arası ağ atlaması geçiyor demektir — Faz 6'nın service mesh
adımı tam olarak bu atlamayı mTLS'e sokacak.
