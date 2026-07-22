# Shardlands

2D top-down mini-MMO: kalıcı, shard'lanmış bir hub dünyası + talep üzerine
açılan gerçek zamanlı arena instance'ları. Amaç, dağıtık sistem
konseptlerini (konsensüs, event sourcing, sharding, actor model, CRDT,
observability...) üretim kalitesinde ama öğrenme odaklı bir projede uçtan
uca uygulamak.

## Monorepo Yapısı

```
pkg/actor/     Faz 0: sıfırdan actor framework (mailbox, supervision)  ✅
pkg/ringbuf/   Faz 0: lock-free MPSC ring buffer                       ✅
pkg/storage/   Faz 0: LSM-tree storage engine                          ✅
pkg/raft/      Faz 0: Raft konsensüs                                   ✅
pkg/clock/     Faz 0: Lamport / vector clock                           ✅
pkg/auth/      Faz 1: minimal HS256 JWT (stdlib)                       ✅
pkg/es/        Faz 2: event store (LSM üstünde, CQRS/ES)               ✅
pkg/crdt/      Faz 2: G/PN-Counter CRDT                                ✅
pkg/hashring/  Faz 3: consistent hashing (vnode'lu)                    ✅
pkg/raftstore/ Faz 3: Raft Storage'ın LSM tabanlı kalıcı hâli          ✅
pkg/dlock/     Faz 3: Raft üstünde lease+fencing dağıtık kilit         ✅
pkg/bus/       Faz 4: event bus (NATS JetStream) + DLQ/backpressure    ✅
pkg/resilience/Faz 4: circuit breaker + bulkhead                       ✅
pkg/ratelimit/ Faz 4: token bucket (rate limit / load shedding)        ✅
proto/         Faz 1: gRPC/Protobuf kontratları (buf ile codegen)      ✅
gen/           Üretilen Go kodu (commit'li — araçsız build için)       ✅
services/      Faz 1+: gateway, player, world, matchmaking, server     ✅
operator/      Faz 5: arena instance Kubernetes operator'ü             ⬜
client/        HTML5 Canvas + vanilla JS istemci                       ✅
```

## Faz 0 — Temel Yapı Taşları ✅ (tag: `faz0`)

| Bileşen | Durum | Notlar |
|---|---|---|
| Actor framework | ✅ | [pkg/actor](pkg/actor/README.md) — mailbox, supervision, restart stratejileri |
| Lock-free ring buffer | ✅ | [pkg/ringbuf](pkg/ringbuf/README.md) — Vyukov MPSC, mailbox'a entegre, kanaldan ~6× hızlı |
| LSM-tree storage engine | ✅ | [pkg/storage](pkg/storage/README.md) — skip list memtable, SSTable+bloom, WAL, manifest, compaction |
| Raft | ✅ | [pkg/raft](pkg/raft/README.md) — leader election + log replication, partition/chaos testleri |
| Logical clocks | ✅ | [pkg/clock](pkg/clock/README.md) — Lamport (atomic) + vector clock (nedensellik/eşzamanlılık) |

### Faz 0 kapanış — öne çıkan dersler

Ayrıntılar paket README'lerinde; kesitler:

1. **Sınır durumları tasarımın parçası.** Ring buffer'da cap-1 seq
   çakışması canlı deadlock olarak yakalandı; invariant yazıya dökülünce
   asgari kapasite kendiliğinden çıktı.
2. **Doğruluk çoğu kez sıralamada yaşıyor.** Aktör kapanışında
   dead→drain→close; LSM'de WAL→memtable, fsync→manifest; Raft'ta
   persist→cevap. Her biri "arada crash olursa?" sorusuyla test edildi.
3. **Flaky test hediyedir.** Üç gerçek hata (actor kapanış yarışı,
   cap-1, Windows delete-pending) önce test kararsızlığı olarak göründü.
4. **Bölünme birinci sınıf senaryodur.** Raft'ta partition, transport
   arayüzüne gömüldü; bütün ilginç davranışlar bölünmede yaşanıyor.
5. **Platform semantiği taşınabilir değil.** Windows'ta açık/yeni
   kapanmış dosya silme kısıtları iki gerçek düzeltme çıkardı
   (O_TRUNC'lı WAL reset, kapatmadan silmeme disiplini).

### Actor framework mimarisi

```mermaid
graph TD
    S[System] --> G["/user guardian"]
    G --> A["aktör (goroutine)"]
    G --> B["aktör (goroutine)"]
    A --> C["çocuk aktör"]
    subgraph Process["process (her aktör için)"]
        MB["user mailbox<br/>(lock-free MPSC ring buffer)"] --> LOOP["run loop"]
        CQ["ctrl queue<br/>(unbounded, öncelikli)"] --> LOOP
        LOOP --> ACT["Actor.Receive<br/>(panic -> supervision)"]
    end
```

## Faz 1 — Çekirdek İskelet, Tek Node (devam ediyor)

Monolit prototip: tüm servisler TEK süreçte ama GERÇEK ağ sınırlarıyla
(player/matchmaking ayrı TCP portlarında gRPC, gateway gerçek gRPC
istemcisi). Faz 4'teki strangler-fig anlatısının "önce" hali.

```mermaid
graph LR
    C1[sekme 1] -- "WS (JSON)" --> GW[gateway / BFF]
    C2[sekme 2] -- "WS (JSON)" --> GW
    GW -- gRPC --> P[player-service<br/>kimlik + JWT basımı]
    GW -- gRPC --> MM[matchmaking-service<br/>kuyruk iskeleti]
    GW -- "aktör mesajları<br/>(session aktörleri)" --> W[world aktörü<br/>hub simülasyonu]
    T[["tick 20Hz"]] --> W
```

- **Kimlik:** `POST /api/login` → player-service oyuncu yaratır, HS256
  JWT basar ([pkg/auth](pkg/auth/jwt.go)); gateway WS el sıkışmasında doğrular.
- **Session'lar:** her WS bağlantısı bir aktör; WS yazmaları aktör
  goroutine'inden (gorilla "tek yazar" kuralı bedavaya), yavaş istemcide
  kare düşer (DropNewest) — dünya asla beklemez.
- **Hareket:** sunucu-otoriter; istemci yalnızca basılı tuş durumunu
  gönderir, dünya 20Hz tick'te fiziği işler ve tam snapshot yayınlar
  (delta/AOI Faz 5).
- **E2E dilim:** "iki sekme birbirinin hareketini görür" hem otomatik
  testte ([server_test.go](services/server/server_test.go)) hem canlı
  tarayıcıda doğrulandı.

## Faz 2 — Kalıcı Hub ve Veri Modeli (devam ediyor)

| Parça | Durum | Notlar |
|---|---|---|
| Event store | ✅ | [pkg/es](pkg/es/README.md) — pkg/storage üstünde; atomik batch, optimistic concurrency, subscribe |
| Chat dilimi (CQRS/ES) | ✅ | komut → world aktörü → ChatSaid event → balon + [read model](services/chat/history.go) + `/api/chat/recent`; restart'ta geçmiş kalıcı |
| Kaynak toplama + envanter | ✅ | respawn'lı node'lar, `ResourceGathered` → `inv-<player>` stream'i → [envanter read model](services/inventory/inventory.go) + `/api/inventory` |
| Trade saga | ✅ | [services/trade](services/trade/README.md) — koreografi + orkestrasyon; rezervasyon, telafi, idempotentlik; `/api/trade` |
| CRDT global sayaçlar | ✅ | [pkg/crdt](pkg/crdt/README.md) — G/PN-Counter, merge özellikleri testli; [services/stats](services/stats/stats.go) toplam toplanan (G-Counter) + `/api/stats` |

### Faz 2 mimarisi (CQRS + Event Sourcing)

```mermaid
graph TD
    subgraph Yazı["Yazı yolu (komut)"]
        WS[WS komutu] --> WA[world aktörü]
        WA -->|doğrula| EV[("event store<br/>pkg/es → LSM")]
        TR[trade saga] --> EV
    end
    EV -->|es.Project<br/>checkpoint akışı| RM
    subgraph RM["Read model'ler (sorgu — CQRS)"]
        CH[chat History]
        IN[inventory bakiye]
        ST[stats G-Counter]
    end
    RM --> API["/api/* sorgu endpoint'leri"]
    API --> C[istemci HUD/panel]
```

Yazı ve okuma tamamen ayrı; ikisini yalnız event log bağlar. Read
model'ler her açılışta log'dan sıfırdan kurulur.

### Faz 2 kapanış — dersler

- **Türetilemeyeni persist et.** Event log tek gerçek; read model'ler,
  es indeksi, CRDT sayaçları hep türetilebilir → persist edilmez, replay
  ile kurulur. (LSM'deki MANIFEST tersi: dosya listesi türetilemez.)
- **Idempotentlik, event dünyasının vergisi.** At-least-once teslim +
  restart replay = her şey tekrar-güvenli olmalı: es batch (tek anahtar),
  saga adımları (tradeID key), read model'ler (0'dan replay).
- **Doğru veri tipi protokolü kaldırır.** Takas Raft-vari koordinasyon
  ister (saga); toplam sayaç CRDT ile lidersiz yakınsar. Problemi araca
  göre değil, aracı probleme göre seç.
- **Dürüst dual-write borcu.** Chat balonu (dünya durumu) + ChatSaid
  (log) iki yazma; süreç içi tek yazarla risk düşük, gerçek çözüm
  (outbox/bus) Faz 4'e yazıldı — gizlenmedi.
- **MVCC'nin event-store hâli.** Event'ler değişmez olduğundan bir
  okuyucunun gördüğü [1..checkpoint] sonsuza dek sabit; versiyon = log
  pozisyonu. Genel amaçlı snapshot isolation (Scan hâlâ kilitli) Faz 3.

## Faz 3 — Sharding ve Dağıtım ✅

| Parça | Notlar |
|---|---|
| Consistent hashing | [pkg/hashring](pkg/hashring/README.md) — vnode'lu halka; minimal remap testli |
| Bölge-shard'lı dünya | 2×2 grid bölge aktörleri; bölge→shard eşlemesi; sınırda **handoff** |
| Shard lideri = Raft | [services/shard](services/shard/shard.go) — shard başına Raft grubu; liderlik sahiplik demek |
| Kalıcı Raft depo | [pkg/raftstore](pkg/raftstore/store.go) — Faz 0 Raft + Faz 0 LSM birleşimi |
| Distributed lock | [pkg/dlock](pkg/dlock/README.md) — lease + **fencing token** |
| CAP/PACELC deneyi | [docs/cap-pacelc.md](docs/cap-pacelc.md) — bilinçli izolasyon |
| 2PC/3PC vs saga | [docs/2pc-vs-saga.md](docs/2pc-vs-saga.md) |

```mermaid
graph TD
    C[istemci] -->|WS| S["session aktörü<br/>(bölge ref'i taşır)"]
    S -->|input| R1["bölge r-0-0"]
    S -.->|handoff sonrası| R2["bölge r-1-1"]
    subgraph SH0["shard-0 (Raft grubu, 3 replika)"]
        R1
        R3["bölge r-0-1"]
    end
    subgraph SH1["shard-1 (Raft grubu, 3 replika)"]
        R2
        R4["bölge r-1-0"]
    end
    HR["consistent hashing<br/>bölge → shard"] --> SH0
    HR --> SH1
    SH0 -->|lider yoksa| F["bölgeler DONAR (CP)"]
    ST["/api/stats (CRDT)"] --> AP["bölünmede de çalışır (AP)"]
```

### Faz 3 kapanış — dersler

- **Consistent hashing "minimal" der, "sıfır" demez.** Ekleme yine ~1/N
  anahtar taşır; sıfır taşınma açık atama tablosu (yani merkezi state =
  konsensüs) ister. Biz ikisini birleştirdik: ring hızlı varsayılan
  eşleme, Raft otoriter sahiplik.
- **"Lider sanıyorum" ≠ "hizmet verebiliyorum".** Bölünmüş lider commit
  edemez ama kendini lider sanar; `QuorumActive` (lease) olmadan
  kullanılabilirlik ölçümü yalan söyler. Bu, CAP deneyinin ön koşuluydu.
- **Yeni lider no-op yazmalı.** §5.4.2 gereği lider yalnız kendi
  döneminden bir kaydı commit edebilir; no-op olmadan önceki dönemin
  kayıtları uygulanmaz ve lider onları **okuyamaz** (dlock'ta "kilit
  kayboldu" olarak patladı).
- **Lease tek başına yetmez, fencing token gerekir.** Duraklamış bir
  sahip lease'i dolduktan sonra hâlâ yazmaya çalışabilir; korunan kaynak
  eski token'ı reddetmeli.
- **CAP bir anahtar değil bütçedir.** Aynı sistemde bölge simülasyonu CP
  (donar), global sayaç AP (çalışır). PACELC profilimiz **PC/EL**:
  bölünmede tutarlılık, normalde gecikme.
- **Sharding = ölçek + arıza yalıtımı.** Bir shard'ın izolasyonu
  diğerinin bölgelerini etkilemiyor; blast radius sınırlı.

## Faz 4 — Mesajlaşma ve Dayanıklılık ✅

| Parça | Notlar |
|---|---|
| Event bus | [pkg/bus](pkg/bus/bus.go) — NATS JetStream; ack/nak, yeniden teslim, **DLQ**, MaxInFlight ile backpressure |
| Outbox relay | [services/outbox](services/outbox/outbox.go) — dual-write borcu kapandı; store tek yazma yeri, relay bus'a taşır |
| Idempotency | [outbox.Consume](services/outbox/consume.go) — global sıra ile dedupe; at-least-once'ın karşılığı |
| Circuit breaker + bulkhead | [pkg/resilience](pkg/resilience/breaker.go) — kaskad arıza gateway sınırında kesilir |
| Rate limit / load shedding | [pkg/ratelimit](pkg/ratelimit/ratelimit.go) — token bucket; IP başına giriş, bağlantı başına komut |
| Strangler fig | [docs/strangler-fig.md](docs/strangler-fig.md) — Faz 1'den bugüne önce/sonra |

### Faz 4 kapanış — dersler

- **Exactly-once teslim yoktur; idempotent tüketici vardır.** Bus
  at-least-once verir; doğruluk, tüketicinin global sıra ile dedupe
  etmesinden gelir. Yayın tarafı dedupe (Msg-Id) tekrarları *azaltır*,
  garanti etmez.
- **Outbox, dual-write'ın tek dürüst çözümü.** "Hem yaz hem yayınla"
  iki sistem arasında atomiklik ister — yoktur. Tek yere yazıp
  ayrı bir relay ile taşımak, kaybı gecikmeye çevirir.
- **Zehirli mesaj akışı tıkamamalı.** MaxDeliver tükenince mesajı DLQ'ya
  *taşımak* gerekir; JetStream'in max-deliver aşımı mesajı sessizce
  düşürür — bir yere koymaz. Kararı kendimiz veriyoruz.
- **In-memory read model = geçici tüketici.** Kalıcı tüketici kaldığı
  yerden devam eder ve süreç yeniden başlayınca geçmiş kaybolur; akışı
  baştan oynatan geçici tüketici doğrusudur.
- **Bulkhead dışta, breaker içte.** Yük atmadan doğan hatalar
  (ErrFull) bizim doymuşluğumuzdur; devre kesiciye yedirilirse sağlıklı
  bir bağımlılık cezalandırılır. Aynı sebeple **istemci hataları**
  (geçersiz girdi) devreyi açmamalı.
- **Yük atmak bir hizmettir.** Hepsini kabul edip topluca çökmektense
  fazlasını hızlıca reddetmek; `Retry-After` ile istemciye ne yapacağını
  söylemek.

## Çalıştırma

```powershell
go run ./cmd/server        # http://localhost:8080 — iki sekme aç
go test -race ./...        # tüm testler
```

Proto codegen (kontrat değişince): `buf generate` — araçlar:
`go install github.com/bufbuild/buf/cmd/buf@latest`,
`google.golang.org/protobuf/cmd/protoc-gen-go@latest`,
`google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`.
Üretilen kod `gen/` altında commit'lidir (araçsız `go build` çalışsın diye).
