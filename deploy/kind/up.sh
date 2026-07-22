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
  docker build -f deploy/docker/Dockerfile.server   -t "shardlands/server:$TAG"   .
  docker build -f deploy/docker/Dockerfile.arena    -t "shardlands/arena:$TAG"    .
  docker build -f deploy/docker/Dockerfile.operator -t "shardlands/operator:$TAG" .
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  echo "==> kind kümesi yaratılıyor"
  kind create cluster --config deploy/kind/kind-config.yaml
fi

echo "==> imajlar düğümlere yükleniyor"
kind load docker-image --name "$CLUSTER" \
  "shardlands/server:$TAG" "shardlands/arena:$TAG" "shardlands/operator:$TAG"

echo "==> CRD kuruluyor"
# CRD ÖNCE gelmeli: sunucu ve operator, Arena tipini tanımayan bir API
# sunucusuna karşı başlatılırsa cache senkronizasyonunda hata alır.
kubectl apply -f operator/config/crd/
kubectl wait --for=condition=Established --timeout=60s \
  crd/arenas.shardlands.dev

echo "==> manifestler uygulanıyor"
kubectl apply -f deploy/k8s/base/
kubectl apply -f deploy/k8s/local/

echo "==> hazır olunuyor"
kubectl -n shardlands rollout status statefulset/nats --timeout=120s
kubectl -n shardlands rollout status statefulset/shardlands --timeout=180s
kubectl -n shardlands rollout status deployment/shardlands-operator --timeout=120s

echo
echo "hub hazır: http://localhost:30080"
echo "arenaları izle: kubectl -n shardlands get arenas -w"
