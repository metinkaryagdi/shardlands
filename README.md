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
pkg/raft/      Faz 0: Raft konsensüs                                   ⬜
pkg/clock/     Faz 0: Lamport / vector clock                           ⬜
services/      Faz 1+: gateway, player, world, matchmaking             ⬜
operator/      Faz 5: arena instance Kubernetes operator'ü             ⬜
client/        HTML5 Canvas + vanilla JS istemci                       ⬜
```

## Faz 0 — Temel Yapı Taşları (devam ediyor)

| Bileşen | Durum | Notlar |
|---|---|---|
| Actor framework | ✅ | [pkg/actor](pkg/actor/README.md) — mailbox, supervision, restart stratejileri |
| Lock-free ring buffer | ✅ | [pkg/ringbuf](pkg/ringbuf/README.md) — Vyukov MPSC, mailbox'a entegre, kanaldan ~6× hızlı |
| LSM-tree storage engine | ✅ | [pkg/storage](pkg/storage/README.md) — skip list memtable, SSTable+bloom, WAL, manifest, compaction |
| Raft | ⬜ | |
| Logical clocks | ⬜ | |

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

## Çalıştırma

```powershell
go test -race ./...
```
