#!/usr/bin/env bash
# DENEY 1 ve 2: Pod öldürme.
#
# İki iş yükü, iki farklı hipotez. Aynı fayın farklı sonuç vermesi
# tesadüf değil, mimarinin sonucu: player kopyalanabilir, hub değil.
source "$(dirname "$0")/lib.sh"

hipotez "1. player Pod'u öldür" \
  "giriş yolu HİÇ kesilmez (2 kopya, maxUnavailable=0, PDB, preStop)"

hedef=$(kubectl -n $NS get pod -l app=shardlands-player \
  -o jsonpath='{.items[0].metadata.name}')
adim "hedef: $hedef"
( sleep 5; kubectl -n $NS delete pod "$hedef" --wait=false >/dev/null ) &
read -r ok fail <<<"$(steady_state 40)"
wait
adim "başarılı=$ok başarısız=$fail"
if [ "$fail" -eq 0 ]; then
  gecti "kesinti yok"
else
  kaldi "$fail istek düştü"
fi

hipotez "2. hub Pod'u öldür" \
  "kısa kesinti olur; event store bozulmaz, sistem KENDİ KENDİNE döner"

adim "hedef: shardlands-0 (tek kopya)"
# ÖLÇÜM TUZAĞI: `delete pod` asenkrondur. Silme komutundan hemen sonra
# "ne zaman döndü" diye ölçmeye başlarsak, kesinti daha başlamadan
# 200 alır ve "0sn kesinti" gibi YANLIŞ bir sonuç yazarız (ilk
# denemede tam olarak bu oldu). Doğrusu: kesintiyi baştan sona kapsayan
# sürekli bir ölçüm yapmak ve DÜŞEN İSTEKLERİ saymak. Saniyede bir
# yoklama yaptığımız için başarısız sayısı ≈ kesinti saniyesi.
( sleep 5; kubectl -n $NS delete pod shardlands-0 --wait=false >/dev/null ) &
read -r ok fail <<<"$(steady_state 90)"
wait
adim "başarılı=$ok başarısız=$fail (≈${fail}sn kesinti)"
if [ "$fail" -eq 0 ]; then
  kaldi "hiç kesinti ölçülmedi — ölçüm fayı yakalamamış olabilir"
elif [ "$fail" -le 30 ]; then
  gecti "kesinti oldu ve sistem KENDİ KENDİNE döndü (~${fail}sn)"
else
  kaldi "kesinti çok uzun: ${fail}sn"
fi

# Event store bozulmadıysa sunucu açılışta WAL'ı oynatıp devam eder;
# bozulsaydı CrashLoopBackOff görürdük. Yeniden başlatma sayısı bunu
# gösterir: 0 = tek seferde açıldı.
restarts=$(kubectl -n $NS get pod shardlands-0 \
  -o jsonpath='{.status.containerStatuses[?(@.name=="server")].restartCount}')
if [ "${restarts:-0}" -eq 0 ]; then
  gecti "event store sağlam (yeniden başlatma yok, WAL oynatıldı)"
else
  kaldi "sunucu $restarts kez yeniden başladı — kurtarma sorunlu"
fi
