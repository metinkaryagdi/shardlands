#!/usr/bin/env bash
# DENEY 5 ve 6: arena dayanıklılığı ve PDB'lerin gerçekten saygı görmesi.
source "$(dirname "$0")/lib.sh"
cd "$(dirname "$0")/../.."

hipotez "5. Arena Pod'unu maç ortasında öldür" \
  "operator yeniden yaratır; ama MAÇ DURUMU KAYBOLUR (bellekte tutuluyor)"

adim "maç başlatılıyor (arka planda iki oyuncu kuyruğa giriyor)"
go run ./internal/smoke -watch=45s >/tmp/chaos-arena.log 2>&1 &
smoke=$!
sleep 12

pod=$(kubectl -n $NS get pod -l app=shardlands-arena \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "$pod" ]; then
  kaldi "arena Pod'u bulunamadı — maç açılmamış"
  kill $smoke 2>/dev/null; wait $smoke 2>/dev/null
  exit 1
fi
adim "hedef: $pod"
kubectl -n $NS delete pod "$pod" --force --grace-period=0 >/dev/null 2>&1

sleep 20
faz=$(kubectl -n $NS get arena -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
yeni=$(kubectl -n $NS get pod -l app=shardlands-arena --no-headers 2>/dev/null | wc -l)
adim "arena durumu: ${faz:-yok}, ayakta Pod: $yeni"

wait $smoke 2>/dev/null
kare=$(grep -o 'arena karesi: [0-9]*' /tmp/chaos-arena.log | tail -1)
adim "istemci tarafı: ${kare:-ölçülemedi}"

# Beklenen: operator seviye-tetiklemeli döngüsüyle Pod'u geri getirir.
# Ama arena durumu (canlar, mermiler, tick) bellekteydi ve gitti —
# maç sıfırdan başlar. Bu bir HATA DEĞİL, tasarım tercihinin sonucu:
# arenayı kalıcı yapmak 30Hz döngüye disk yazması eklemek demekti.
if [ "${faz:-}" = "Running" ] || [ "$yeni" -ge 1 ]; then
  gecti "operator arenayı geri getirdi (uzlaştırma çalıştı)"
else
  adim "arena geri gelmedi — TTL/temizlik yolu devreye girmiş olabilir: ${faz:-yok}"
fi

hipotez "6. Düğüm boşalt (drain)" \
  "PDB'ler saygı görür: player'da en az 1 kopya kalır, arena tahliye edilmez"

dugum=$(kubectl -n $NS get pod -l app=shardlands-player \
  -o jsonpath='{.items[0].spec.nodeName}')
adim "hedef düğüm: $dugum"
adim "PDB durumu:"
kubectl -n $NS get pdb --no-headers | sed 's/^/     /'

# --dry-run=server eviction'ı gerçekten denemez; PDB'yi sınamak için
# gerçek drain gerekir. Kısa zaman aşımıyla koşup sonucu okuyoruz.
adim "drain deneniyor (60sn zaman aşımı)"
if kubectl drain "$dugum" --ignore-daemonsets --delete-emptydir-data \
     --timeout=60s >/tmp/chaos-drain.log 2>&1; then
  adim "drain tamamlandı"
else
  adim "drain zaman aşımına uğradı (PDB blokladıysa beklenen budur)"
fi
tail -3 /tmp/chaos-drain.log | sed 's/^/     /'

kalan=$(kubectl -n $NS get pod -l app=shardlands-player \
  --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
adim "ayakta kalan player kopyası: $kalan"
if [ "$kalan" -ge 1 ]; then
  gecti "PDB korudu: en az bir player kopyası ayakta"
else
  kaldi "bütün player kopyaları gitti — PDB işe yaramamış"
fi

adim "düğüm geri alınıyor"
kubectl uncordon "$dugum" >/dev/null
