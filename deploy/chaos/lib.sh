#!/usr/bin/env bash
# Kaos deneyleri için ortak yardımcılar.
#
# Kalıp her deneyde aynı: HİPOTEZ YAZ → ölç → boz → ölç → hüküm ver.
# Hipotez önce yazılmazsa deney değil log izlemedir (docs/chaos.md §1).
set -uo pipefail

NS=shardlands
BASE=${BASE:-http://localhost:30080}

hipotez() { printf '\n\033[1m=== %s\033[0m\n    hipotez: %s\n' "$1" "$2"; }
adim()    { printf '  -> %s\n' "$1"; }
gecti()   { printf '  \033[32mGEÇTİ\033[0m: %s\n' "$1"; }
kaldi()   { printf '  \033[31mKALDI\033[0m: %s\n' "$1"; }

# steady_state <saniye>, giriş yolunun kararlı durumunu ölçer.
# Çıktı: "<basarili> <basarisiz>"
steady_state() {
  local sure=$1 ok=0 fail=0 son
  local bitis=$(( $(date +%s) + sure ))
  while [ "$(date +%s)" -lt "$bitis" ]; do
    son=$(curl -s -o /dev/null -w '%{http_code}' -m 5 \
      -X POST -H 'Content-Type: application/json' \
      -d '{"name":"kaos"}' "$BASE/api/login" 2>/dev/null || echo 000)
    if [ "$son" = "200" ]; then ok=$((ok+1)); else fail=$((fail+1)); fi
    sleep 1
  done
  echo "$ok $fail"
}

# kurtarma_suresi <azami saniye>, giriş yolu tekrar 200 dönene kadar
# geçen süreyi saniye cinsinden yazar; süre dolarsa -1.
kurtarma_suresi() {
  local azami=$1 basla now son
  basla=$(date +%s)
  while :; do
    son=$(curl -s -o /dev/null -w '%{http_code}' -m 3 \
      -X POST -H 'Content-Type: application/json' \
      -d '{"name":"kurtarma"}' "$BASE/api/login" 2>/dev/null || echo 000)
    now=$(date +%s)
    if [ "$son" = "200" ]; then echo $(( now - basla )); return; fi
    if [ $(( now - basla )) -ge "$azami" ]; then echo -1; return; fi
    sleep 1
  done
}
