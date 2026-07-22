#!/usr/bin/env bash
# Shardlands'i yerel bir kind kümesinde ayağa kaldırır.
#
#   ./deploy/kind/up.sh          # küme + imajlar + manifestler
#   ./deploy/kind/up.sh --skip-build   # imajları yeniden derleme
#
# Neden "kind load" ve imPullPolicy: IfNotPresent? kind düğümleri ana
# makinenin Docker imaj deposunu GÖRMEZ; imajı düğümün içine ayrıca
# yüklemek gerekir. Aksi halde Pod'lar ImagePullBackOff'ta takılır ve
# Kubernetes'e yeni geçenlerin en sık düştüğü tuzak budur.
set -euo pipefail

cd "$(dirname "$0")/../.."
CLUSTER=shardlands
TAG=${TAG:-dev}

if [[ "${1:-}" != "--skip-build" ]]; then
  echo "==> imajlar derleniyor (tag: $TAG)"
  for img in server player arena operator smoke; do
    docker build -f "deploy/docker/Dockerfile.$img" -t "shardlands/$img:$TAG" .
  done
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  echo "==> kind kümesi yaratılıyor"
  kind create cluster --config deploy/kind/kind-config.yaml
fi

echo "==> imajlar düğümlere yükleniyor"
kind load docker-image --name "$CLUSTER" \
  "shardlands/server:$TAG" "shardlands/player:$TAG" "shardlands/arena:$TAG" \
  "shardlands/operator:$TAG" "shardlands/smoke:$TAG"

echo "==> CRD kuruluyor"
# CRD ÖNCE gelmeli: sunucu ve operator, Arena tipini tanımayan bir API
# sunucusuna karşı başlatılırsa cache senkronizasyonunda hata alır.
kubectl apply -f operator/config/crd/
kubectl wait --for=condition=Established --timeout=60s \
  crd/arenas.shardlands.dev

# SIRA ÖNEMLİ: namespace → mesh politikaları → iş yükleri.
#
# Politikalar iş yüklerinden SONRA uygulanırsa, varsayılan "deny"
# altında ayağa kalkmış proxy'ler bağlantıları reddetmeye devam eder ve
# ancak yeniden başlatılınca düzelirler. Yaşanmış: hub→player çağrıları
# "error reading server preface: EOF" ile düştü, politika doğru olduğu
# halde. CRD tuzağının kardeşi — önce kural, sonra iş yükü.
echo "==> namespace"
kubectl apply -f deploy/k8s/base/00-namespace.yaml

# Mesh politikaları YALNIZ Linkerd kuruluysa uygulanır. Kurulu değilse
# CRD'leri olmadığı için apply hata verir; mesh'siz kurulum da geçerli
# bir çalışma biçimi olduğu için bu adımı sessizce atlıyoruz.
if kubectl get crd servers.policy.linkerd.io >/dev/null 2>&1; then
  echo "==> mesh politikaları uygulanıyor"
  kubectl apply -f deploy/k8s/mesh/00-identities.yaml \
    -f deploy/k8s/mesh/10-policy-player.yaml \
    -f deploy/k8s/mesh/11-policy-arena.yaml \
    -f deploy/k8s/mesh/12-policy-nats.yaml \
    -f deploy/k8s/mesh/13-policy-hub.yaml
else
  echo "==> Linkerd kurulu değil, mesh politikaları atlandı"
  echo "    (kurmak için: ./deploy/mesh/install.sh)"
fi

echo "==> manifestler uygulanıyor"
kubectl apply -f deploy/k8s/base/
kubectl apply -f deploy/k8s/local/

echo "==> hazır olunuyor"
kubectl -n shardlands rollout status statefulset/nats --timeout=120s
kubectl -n shardlands rollout status deployment/player --timeout=120s
kubectl -n shardlands rollout status statefulset/shardlands --timeout=180s
kubectl -n shardlands rollout status deployment/shardlands-operator --timeout=120s

echo
echo "hub hazır: http://localhost:30080"
echo "arenaları izle: kubectl -n shardlands get arenas -w"
