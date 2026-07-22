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

if (-not $SkipBuild) {
    Write-Host "==> imajlar derleniyor (tag: $tag)"
    docker build -f deploy/docker/Dockerfile.server   -t "shardlands/server:$tag"   .
    if ($LASTEXITCODE -ne 0) { throw "server imajı derlenemedi" }
    docker build -f deploy/docker/Dockerfile.arena    -t "shardlands/arena:$tag"    .
    if ($LASTEXITCODE -ne 0) { throw "arena imajı derlenemedi" }
    docker build -f deploy/docker/Dockerfile.operator -t "shardlands/operator:$tag" .
    if ($LASTEXITCODE -ne 0) { throw "operator imajı derlenemedi" }
}

$existing = kind get clusters
if ($existing -notcontains $cluster) {
    Write-Host "==> kind kümesi yaratılıyor"
    kind create cluster --config deploy/kind/kind-config.yaml
    if ($LASTEXITCODE -ne 0) { throw "küme yaratılamadı" }
}

Write-Host "==> imajlar düğümlere yükleniyor"
kind load docker-image --name $cluster "shardlands/server:$tag" "shardlands/arena:$tag" "shardlands/operator:$tag"

Write-Host "==> CRD kuruluyor"
kubectl apply -f operator/config/crd/
kubectl wait --for=condition=Established --timeout=60s crd/arenas.shardlands.dev

Write-Host "==> manifestler uygulanıyor"
kubectl apply -f deploy/k8s/base/
kubectl apply -f deploy/k8s/local/

Write-Host "==> hazır olunuyor"
kubectl -n shardlands rollout status statefulset/nats --timeout=120s
kubectl -n shardlands rollout status statefulset/shardlands --timeout=180s
kubectl -n shardlands rollout status deployment/shardlands-operator --timeout=120s

Write-Host ""
Write-Host "hub hazır: http://localhost:30080"
Write-Host "arenaları izle: kubectl -n shardlands get arenas -w"
