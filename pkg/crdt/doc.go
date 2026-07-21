// Package crdt, çakışmasız replika veri tipleri (Conflict-free Replicated
// Data Types) sağlar: G-Counter (yalnız artan) ve PN-Counter (artan +
// azalan).
//
// CRDT'ler, Raft'ın TAM KARŞITIDIR. Raft anlaşma (consensus) makinesidir:
// N kopya tek bir doğru sıra üzerinde uzlaşır; lider, çoğunluk, bölünmede
// duraklama gerekir (CAP'te C tarafı). CRDT ise anlaşma GEREKTİRMEYEN
// veri içindir: merge işlemi
//
//	commutative  (a⊔b = b⊔a)          — mesaj sırası önemsiz
//	associative  ((a⊔b)⊔c = a⊔(b⊔c))  — gruplama önemsiz
//	idempotent   (a⊔a = a)            — tekrar/çift teslim zararsız
//
// olacak biçimde bir join-semilattice kurulursa, kopyalar mesajları hangi
// sıra/kaç kez alırsa alsın AYNI değere yakınsar (Strong Eventual
// Consistency). Koordinatör, lider, çoğunluk yoktur; bölünmede bile yazma
// devam eder, sonra birleşir (CAP'te A tarafı — AP).
//
// Vector clock ile ilişki: bir G-Counter map[node]sayaç'tır ve merge'ü
// ELEMAN-BAZLI MAX'tır — bu, pkg/clock'taki vector clock'un merge
// kuralının aynısıdır. Vector clock sonucu karşılaştırır (nedensellik),
// G-Counter toplar (değer); ikisi de aynı semilattice üstünde yaşar.
//
// Bu tip STATE-BASED (CvRDT)'dir: kopyalar tüm durumu gönderip merge eder.
// Operation-based (CmRDT) alternatifi mesajları taşır ama exactly-once
// teslim varsayar; state-based, at-least-once ağlara doğal uyar
// (idempotentlik sayesinde). Shardlands için (Faz 4 event bus, kayıplı
// teslim) state-based doğru seçim.
package crdt
