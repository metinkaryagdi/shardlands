# Sır Yönetimi — Vault, sır sıfırı ve rotasyon

Faz 6'nın beşinci adımı. Konu JWT imzalama anahtarı: onu ele geçiren
**herkesin adına geçerli token basabilir**.

## 1. Kubernetes Secret neden sır değildir

`kind: Secret` adı yanıltıcıdır. Gerçekte:

- **Şifreli değil, base64.** `kubectl get secret -o yaml | base64 -d`
  yeterli. Şifreleme ancak etcd'de "encryption at rest" açıksa vardır
  ve o da varsayılan değildir.
- **RBAC ile okunabilir.** Namespace'te secret okuma yetkisi olan her
  şey görür.
- **GitOps ile Git'e girer.** Ve asıl yıkıcı olan bu: Faz 6'nın GitOps
  adımından sonra sır artık yalnız kümede değil, **Git geçmişinde**
  duruyor. Kümeden silmek kolaydır; geçmişten silmek `filter-repo` +
  zorla push + herkesin klonunu tazelemesi demektir. Pratikte "sızmış"
  sayılır ve **döndürülmesi** gerekir.

Bu son maddenin bu projede somut karşılığı var: `shardlands-secret`
manifesti `dev-secret-change-me` ile commit'lendi ve orada duruyor.
Silmedik — çünkü silmek "çözüldü" yanılsaması üretirdi. Çözülen şey
**üretim yolu**: `VAULT_ADDR` tanımlıyken o Secret kullanılmıyor.

## 2. Sır sıfırı (secret zero) problemi

Uygulama sırları Vault'tan alacak. Peki Vault'a nasıl kimlik
kanıtlayacak? Bir parola verirsek problemi öteledik: o parola nerede
duracak? Sonsuz gerileme.

Kubernetes'in cevabı zarif: **Pod'un ServiceAccount token'ı zaten var**.
kubelet onu dosya sistemine bağlar, kube-apiserver imzalamıştır.

```
Pod ──(SA token)──> Vault ──(TokenReview)──> kube-apiserver
                                                   │
Pod <──(Vault token)── Vault <──("evet, bu SA")────┘
```

Vault kimseye güvenmiyor; token'ı **imzalayana** soruyor. Kimlik yine
iş yükünün kendisinden geliyor, paylaşılan bir sırdan değil.

Bu, mesh adımındaki mTLS kimliğiyle (SPIFFE) **aynı fikrin** başka bir
uygulaması. Faz 6 boyunca üçüncü kez aynı ilkeye çıktık:

| Nerede | Soru | Kanıtı veren |
| --- | --- | --- |
| Faz 3, fencing token | "kilidi hâlâ ben mi tutuyorum?" | Raft grubu |
| Faz 6, mesh mTLS | "sen gerçekten hub musun?" | linkerd-identity |
| Faz 6, Vault | "sana sır verebilir miyim?" | kube-apiserver |

Hepsinin ortak cümlesi: **söylediğine güvenme, kanıtı iste.**

## 3. Teslimat deseni: neden doğrudan API?

Vault'tan sır almanın yaygın yolları:

| Yöntem | Artı | Eksi |
| --- | --- | --- |
| Agent Injector (sidecar) | Uygulama değişmez, dosyaya yazar | Sidecar + webhook; rotasyonu uygulamanın fark etmesi yine gerekir |
| Secrets Store CSI | Volume olarak bağlanır, standart | Sürücü kurulumu; yine dosya izleme gerekir |
| External Secrets Operator | Vault → K8s Secret senkronu | **K8s Secret'ı geri getirir** — çözdüğümüz problemi geri çağırır |
| Doğrudan API | Akış tamamen görünür, ek bileşen yok | İstemci kodu yazmak gerekir |

Bu proje **doğrudan API**'yi seçti ([pkg/vault](../pkg/vault)), çünkü
Faz 0'dan beri kural şu: mekanizmayı önce elle yaz. Burada öğrenilecek
şey bir kütüphane değil, sır sıfırı akışının kendisi — ve o akış iki
HTTP çağrısında tamamen görünür.

Üretimde muhtemelen injector ya da CSI tercih edilirdi: uygulamaya
Vault bağımlılığı sokmamak, kimlik/yenileme mantığını platforma
bırakmak değerlidir.

## 4. ASIL MESELE: rotasyon

Sır depolamak kolaydır. **Değiştirmek** zordur ve "Vault kullanıyoruz"
diyen kurulumların çoğunda sessizce eksik olan parça budur.

### Neden tek anahtarla rotasyon imkânsız

İmzalama anahtarını değiştirdiğin anda daha önce basılmış **bütün**
token'lar geçersiz olur. Oyundaki herkes düşer. Token ömrü 24 saat
olduğu için "eskiler dolsun" demek rotasyonu 24 saate yaymak demektir —
ihlal durumunda kimsenin o kadar vakti yoktur.

### Çözüm: imzada tek, doğrulamada çoklu anahtar

[`pkg/auth.Keyring`](../pkg/auth/keyring.go): `keys[0]` ile imzala,
hepsiyle doğrulamayı dene. Rotasyon üç adıma iner:

```bash
# 1) Yeni anahtar başa, eski doğrulamada kalsın
vault kv put secret/shardlands/jwt \
  jwt_signing_key="$(head -c 32 /dev/urandom | base64)" \
  jwt_previous_keys="<eski anahtar>"

# 2) Token ömrü kadar bekle (burada 24 saat)

# 3) Eskiyi düşür
vault kv patch secret/shardlands/jwt jwt_previous_keys=""
```

Uygulama tarafında yeniden başlatma **yok**: `vault.KeySource` sırrı
periyodik okur (`VAULT_REFRESH`, kümede 20sn) ve zinciri atomik olarak
değiştirir. Okuma yolu `atomic.Pointer` üstünden gittiği için akıştaki
istekler kilitlenmez ve yarım liste görmez.

### Neden `kid` (key id) kullanmıyoruz

JWT header'ına anahtar kimliği koyup doğrudan doğru anahtarı
seçebilirdik. Yapmadık: `pkg/auth`'un güvenlik dayanağı header'ın
**sabit** olması ve token'dan hiçbir şey okumamamız (`alg:none`
panzehiri, bkz. jwt.go). Birkaç anahtarı sırayla denemek birkaç HMAC
hesabına mal olur; header'ı yorumlamaya başlamak saldırı yüzeyi açar.
Anahtar sayısı JWKS ölçeğine çıkarsa bu takas tersine döner.

## 5. Ele geçen bir Pod ne kadar ilerleyebilir?

İki bağımsız kat var ve farklı soruları cevaplıyorlar:

- **Vault politikası**: bu kimlik yalnız `secret/data/shardlands/jwt`
  yolunu **okuyabilir**. Yazamaz, listeleyemez, başka yolu göremez.
- **Vault rolü**: yalnız `shardlands-server` ve `shardlands-player`
  ServiceAccount'ları bu politikayı alır. Başka bir SA giriş bile
  yapamaz.
- **Mesh politikası**: kimliği olmayan bir Pod Vault'un giriş uç
  noktasına **bağlanamaz** bile ([14-policy-vault.yaml](../deploy/k8s/mesh/14-policy-vault.yaml)).

Savunma derinliği: ilki ele geçen kimliğin yapabileceklerini daraltır,
sonuncusu kimliği olmayanın kapıya ulaşmasını engeller.

## 6. Bu kurulumun dürüst eksikleri

- **Vault dev modunda.** Depolama bellekte (Pod ölürse sırlar gider),
  otomatik unseal, sabit root token, TLS yok. Gerçek Vault'ta açılışta
  Shamir payları ya da auto-unseal ile mühür açılır — "sırların sırrı"
  sorusunun asıl cevabı orada.
- **mlock kapalı.** Vault imajı açılışta `IPC_LOCK` almak ister; bizim
  `capabilities: drop: ["ALL"]` duruşumuz buna izin vermiyor.
  `SKIP_SETCAP` ile atlıyoruz ve **bedeli şudur**: Vault'un belleği
  takas alanına yazılabilir, yani sırlar diske düşebilir. Üretimde
  doğru cevap IPC_LOCK vermektir.
- **Sır hâlâ statik.** Vault'un dinamik sırları (veritabanı kimlik
  bilgilerini isteğe göre üretip TTL sonunda iptal etme) bu projede
  kullanılmadı; imzalama anahtarı doğası gereği paylaşılan ve uzun
  ömürlü.
- **Root token manifest içinde.** Yapılandırma Job'ı `VAULT_TOKEN=root`
  ile koşuyor. Gerçek kurulumda bu adım bir insan ya da ayrı bir
  önyükleme mekanizmasıyla yapılır.

## 7. Doğrulama

Rotasyonun iddiası tek cümlede: **yeni anahtar devreye girdiğinde eski
token'lar çalışmaya devam eder.** Bunun iki ayrı kanıtı var:

- Birim testi: [`pkg/auth/keyring_test.go`](../pkg/auth/keyring_test.go)
  — `TestRotationKeepsOldTokensValid` üç adımı da kapsar.
- Kümede uçtan uca: [deploy/README.md](../deploy/README.md)'deki
  rotasyon deneyi.
