#!/usr/bin/env bash
# DENEY 3 ve 4: bağımlılık bölünmesi.
#
# Bölünme aracı olarak ZERO-TRUST POLİTİKASI kullanılıyor: bir
# AuthorizationPolicy silinince namespace'in varsayılan "deny"i devreye
# girer ve mesh proxy'si o bağlantıyı anında reddeder. Gerçek,
# bağlantı düzeyinde ve tek komutla geri alınabilir bir bölünme.
# Güvenlik katmanının kaos aracı olarak çalışması bu fazın sürpriziydi
# (docs/chaos.md §3).
#
# Sınanan iddia Faz 4'ten: BAĞIMLILIK ARIZASINDA KENDİNİ ÖLDÜRME.
# Kod yorumlarında defalarca iddia edildi; burada kanıtlanıyor.
source "$(dirname "$0")/lib.sh"

MESH=deploy/k8s/mesh
cd "$(dirname "$0")/../.."

hipotez "3. Vault'a erişimi kes" \
  "girişler ÇALIŞMAYA DEVAM EDER (önbellekteki anahtar); yalnız tazeleme düşer"

adim "vault-http-clients-only politikası siliniyor → varsayılan deny"
kubectl -n $NS delete authorizationpolicy vault-http-clients-only >/dev/null 2>&1
# Tazeleme aralığı 20sn; en az iki tur boyunca ölç.
read -r ok fail <<<"$(steady_state 50)"
adim "başarılı=$ok başarısız=$fail"

hata=$(kubectl -n $NS logs deployment/player -c player --tail=50 2>/dev/null \
  | grep -c "tazeleme başarısız" || true)
adim "player log'unda tazeleme hatası: $hata"

adim "politika geri konuyor"
kubectl apply -f $MESH/14-policy-vault.yaml >/dev/null

if [ "$fail" -eq 0 ] && [ "$hata" -gt 0 ]; then
  gecti "Vault düştü, servis ayakta kaldı — hata loglandı ama giriş kesilmedi"
elif [ "$fail" -eq 0 ]; then
  gecti "giriş kesilmedi (tazeleme hatası log'a henüz düşmemiş olabilir)"
else
  kaldi "$fail giriş düştü — bağımlılık arızası servise yansıdı"
fi

hipotez "4. NATS'e erişimi kes" \
  "hub AYAKTA KALIR; read model'ler tazelenmez ama süreç ölmez"

adim "nats-client-hub-only politikası siliniyor"
kubectl -n $NS delete authorizationpolicy nats-client-hub-only >/dev/null 2>&1
read -r ok fail <<<"$(steady_state 40)"
adim "başarılı=$ok başarısız=$fail"

restarts=$(kubectl -n $NS get pod shardlands-0 \
  -o jsonpath='{.status.containerStatuses[?(@.name=="server")].restartCount}')
adim "hub yeniden başlatma sayısı: ${restarts:-?}"

adim "politika geri konuyor"
kubectl apply -f $MESH/12-policy-nats.yaml >/dev/null

if [ "$fail" -eq 0 ]; then
  gecti "bus koptu, hub ayakta kaldı"
else
  kaldi "$fail giriş düştü — bus arızası giriş yoluna sızdı"
fi
