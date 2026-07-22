#!/usr/bin/env bash
# Linkerd kontrol düzlemini kurar (kümede mesh yoksa).
#
#   ./deploy/mesh/install.sh
#
# İki aşama, sırası önemli:
#   1) CRD'ler (Server, AuthorizationPolicy, MeshTLSAuthentication...)
#   2) Kontrol düzlemi (identity, destination, proxy-injector)
# CRD'ler önce gelmeli; kontrol düzlemi kendi kaynak tiplerini tanımayan
# bir API sunucusunda açılışta ölür — CRD tuzağının aynısı.
#
# DIŞ İNDİRME UYARISI: linkerd CLI yoksa run.linkerd.io/install'dan
# indirilir. Betiği çalıştırmadan önce bunu bilerek onayla.
set -euo pipefail

if ! command -v linkerd >/dev/null 2>&1; then
  echo "linkerd CLI bulunamadı."
  echo "Kur:  curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh"
  echo "Sonra PATH'e ekle:  export PATH=\$PATH:\$HOME/.linkerd2/bin"
  exit 1
fi

echo "==> ön kontrol"
linkerd check --pre

echo "==> CRD'ler"
linkerd install --crds | kubectl apply -f -

echo "==> kontrol düzlemi"
# Sertifikaları linkerd kendisi üretir (geliştirme). Üretimde kök
# sertifika harici bir CA'dan gelir ve rotasyonu Faz 6'nın Vault adımı
# ile birlikte düşünülür — kimlik altyapısının kökü sırdır.
linkerd install | kubectl apply -f -

echo "==> hazır olunuyor"
linkerd check

echo
echo "kontrol düzlemi hazır. Uygulamayı mesh'e almak için Pod'ları"
echo "yeniden başlat:  kubectl -n shardlands rollout restart statefulset,deployment"
