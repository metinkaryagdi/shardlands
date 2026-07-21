# pkg/raft — Raft Konsensüs

Sıfırdan Raft (Ongaro & Ousterhout). Faz 3'te shard lideri seçimi ve
replikasyon için kullanılacak.

## Problem ve neden Raft?

N kopya, tek gerçek: kopyaların aynı komut dizisi üzerinde anlaşması
(replicated state machine). Paxos aynı problemi çözer ama
anlaşılabilirlik için tasarlanmamıştır; Raft problemi üç anlaşılır
parçaya böler: **leader election**, **log replication**, **safety**.
Alternatifler: Paxos/Multi-Paxos (eşdeğer güç, zor pedagoji),
Viewstamped Replication (Raft'ın ağabeyi), ZAB (ZooKeeper'a özgü).
CRDT'ler ise anlaşma GEREKTİRMEYEN veriler için tamamen farklı bir yol
— Faz 2'de global sayaçlarda kullanacağız; Raft, "tek doğru sıra" şart
olduğunda devreye girer.

## Kilit mekanizmalar (kodda nerede?)

- **Dönem (term) = mantıksal saat.** Her mesaj dönem taşır; yüksek
  dönem gören herkes follower'a düşer (`stepDownLocked`). Dönem asla
  geri gitmez — bu tek kural, eski liderlerin zombiliğini çözer.
- **Randomize seçim zaman aşımı** split vote'u kırar: aynı anda uyanan
  adaylar turu boşa harcar, farklı uyananlarda ilk uyanan kazanır.
- **Oy güvenliği (§5.4.1):** log'u geride olan aday, dönemi yüksek olsa
  bile oy alamaz (`HandleRequestVote` upToDate kontrolü). Commit edilmiş
  kaydı taşımayan biri lider olamaz.
- **Tutarlılık kontrolü + geri yürüme:** AppendEntries prev(index,term)
  eşleşmezse red; lider nextIndex'i geri yürütüp tekrar dener. İlk
  çelişkide follower kuyruğu keser, liderin log'unu alır (idempotent).
- **§5.4.2 commit kuralı:** lider yalnızca KENDİ dönemindeki kaydı
  çoğunluk sayımıyla commit eder (`advanceCommitLocked`); eski dönem
  kayıtları ancak dolaylı commit olur. Figure 8 senaryosunun panzehiri.
- **Persist-then-respond:** currentTerm/votedFor/log her değişimde,
  cevaptan ÖNCE Storage'a iner — crash sonrası aynı dönemde çifte oy
  imkânsızlaşır.

## Tasarım kararları

- RPC'ler `Transport` arayüzünün arkasında; testler partition simüle
  eden in-memory `Network` kullanır (`Partition/Isolate/Heal`).
  Bölünme bir test aracı değil birinci sınıf senaryo — Raft'ın bütün
  ilginç davranışları bölünmede yaşanır. Gerçek gRPC transport Faz 3'te.
- Tek mutex + "kilidi bırakıp RPC at" disiplini (etcd/6.824 stili).
  Kilitli RPC iki düğümün karşılıklı beklemesiyle deadlock üretirdi.
- Zamanlayıcı: tick tabanlı polling (10ms) — timer iptali/sıfırlama
  yarışlarından kaçınmanın en dertsiz yolu.
- `Storage` her seferinde tüm HardState'i yazar (O(log), basit).
  Gerçek motor append-only WAL ister; pkg/storage entegrasyonu Faz 3'te.

## Kapsam dışı (bilinçli)

Snapshot/log compaction (restart eden düğüm TÜM log'u baştan uygular —
testte görünür), üyelik değişikliği (joint consensus), PreVote/lease
read optimizasyonları, batching/pipelining.

## Testler

Her aşamada partition testi var (sona bırakılmadı):

- Seçim: tek lider; lider crash → failover; çoğunluksuz düğüm asla
  lider olamaz; **izole lider iyileşmede çekilir**; oy güvenliği birim
  testi (geride log + yüksek dönem = red).
- Replikasyon: temel replikasyon; **izole follower iyileşince
  yakalar**; **azınlık lideri kabul eder ama commit edemez, kaydı
  iyileşmede silinir** (ana güvenlik senaryosu, 5 düğüm); crash+restart
  HardState'ten döner; AppendEntries çakışma kesme birim testi.
- Chaos: 2 saniyelik rastgele bölünme/iyileşme fırtınası altında
  öneri akışı; sonunda tüm düğümler aynı, çiftsiz diziye yakınsar.

## Learnings

- **Raft'ın zorluğu kuralların sayısı değil, sıralanışı.** "Persist
  cevaptan önce", "commit yalnız kendi döneminden", "oy yalnız güncel
  log'a" — üçü de tek başına küçük görünür; herhangi biri atlanırsa
  güvenlik sessizce çöker ve ancak partition testi yakalar.
- **Cevap eskimiş olabilir.** RPC dönerken dünya değişmiş olabilir:
  her cevap işlenirken "hâlâ aynı dönemde ve aynı roldeyiz mi?"
  kontrolü şart (`n.term != req.Term || n.state != ...`). Bu kontroller
  kodun en kolay unutulan satırları.
- **Test kümesi = ağ modeli.** Partition'ı transport'a gömmek (ayrı
  bir test hilesi yerine) hem testleri okunur yaptı hem de Faz 3'teki
  gerçek ağ geçişinde değişmeyecek bir arayüz bıraktı.
