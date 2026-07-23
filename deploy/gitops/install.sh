#!/usr/bin/env bash
# ArgoCD kontrol düzlemini kurar ve kök Application'ı uygular.
#
#   ./deploy/gitops/install.sh
#
# DIŞ İNDİRME UYARISI: ArgoCD'nin kararlı kurulum manifesti
# raw.githubusercontent.com'dan çekilir.
#
# NOT: ArgoCD kendisi GitOps ile yönetilmiyor — onu kuran bu betik.
# "Kim denetleyiciyi denetler" sorusunun bu kurulumdaki cevabı: kimse.
# Bkz. docs/gitops.md §6.
set -euo pipefail

cd "$(dirname "$0")/../.."
VERSION=${ARGOCD_VERSION:-stable}

echo "==> argocd namespace"
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -

echo "==> ArgoCD kontrol düzlemi ($VERSION)"
kubectl apply -n argocd -f \
  "https://raw.githubusercontent.com/argoproj/argo-cd/$VERSION/manifests/install.yaml"

echo "==> hazır olunuyor"
kubectl -n argocd rollout status deployment/argocd-repo-server --timeout=300s
kubectl -n argocd rollout status deployment/argocd-server --timeout=300s
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=300s

echo "==> kök Application (elle yapılan son iş)"
kubectl apply -f deploy/gitops/root.yaml

cat <<'EOF'

ArgoCD kuruldu.

  Durum:     kubectl -n argocd get applications
  Arayüz:    kubectl -n argocd port-forward svc/argocd-server 8081:443
             https://localhost:8081  (kullanıcı: admin)
  Parola:    kubectl -n argocd get secret argocd-initial-admin-secret \
               -o jsonpath='{.data.password}' | base64 -d

Bundan sonra kümeye elle dokunma: degisiklikleri Git'e yaz.
EOF
