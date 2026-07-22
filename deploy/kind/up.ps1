# Shardlands'i yerel bir kind kümesinde ayağa kaldırır (Windows).
#
#   .\deploy\kind\up.ps1
#   .\deploy\kind\up.ps1 -SkipBuild
#
# up.sh ile aynı adımlar; geliştirme Windows'ta yapıldığı için ikizi var.
param([switch]$SkipBuild)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\..")
$cluster = "shardlands"
$tag = if ($env:TAG) { $env:TAG } else { "dev" }

$images = @("server", "player", "arena", "operator", "smoke")

if (-not $SkipBuild) {
    Write-Host "==> imajlar derleniyor (tag: $tag)"
    foreach ($img in $images) {
        docker build -f "deploy/docker/Dockerfile.$img" -t "shardlands/${img}:$tag" .
        if ($LASTEXITCODE -ne 0) { throw "$img imaji derlenemedi" }
    }
}

$existing = kind get clusters
if ($existing -notcontains $cluster) {
    Write-Host "==> kind kümesi yaratılıyor"
    kind create cluster --config deploy/kind/kind-config.yaml
    if ($LASTEXITCODE -ne 0) { throw "küme yaratılamadı" }
}

Write-Host "==> imajlar düğümlere yükleniyor"
$refs = $images | ForEach-Object { "shardlands/${_}:$tag" }
kind load docker-image --name $cluster @refs

Write-Host "==> CRD kuruluyor"
kubectl apply -f operator/config/crd/
kubectl wait --for=condition=Established --timeout=60s crd/arenas.shardlands.dev

Write-Host "==> manifestler uygulanıyor"
kubectl apply -f deploy/k8s/base/
kubectl apply -f deploy/k8s/local/

# Mesh politikaları yalnız Linkerd kuruluysa; mesh'siz kurulum da
# geçerli bir çalışma biçimi.
kubectl get crd servers.policy.linkerd.io 2>$null | Out-Null
if ($?) {
    Write-Host "==> mesh politikaları uygulanıyor"
    kubectl apply -f deploy/k8s/mesh/00-identities.yaml
    kubectl apply -f deploy/k8s/mesh/10-policy-player.yaml
    kubectl apply -f deploy/k8s/mesh/11-policy-arena.yaml
    kubectl apply -f deploy/k8s/mesh/12-policy-nats.yaml
    kubectl apply -f deploy/k8s/mesh/13-policy-hub.yaml
} else {
    Write-Host "==> Linkerd kurulu degil, mesh politikalari atlandi"
}

Write-Host "==> hazır olunuyor"
kubectl -n shardlands rollout status statefulset/nats --timeout=120s
kubectl -n shardlands rollout status deployment/player --timeout=120s
kubectl -n shardlands rollout status statefulset/shardlands --timeout=180s
kubectl -n shardlands rollout status deployment/shardlands-operator --timeout=120s

Write-Host ""
Write-Host "hub hazır: http://localhost:30080"
Write-Host "arenaları izle: kubectl -n shardlands get arenas -w"
