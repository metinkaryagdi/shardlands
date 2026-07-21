package trade

import (
	"log"
	"strings"

	"shardlands/pkg/es"
)

// Choreographer, takas saga'sını KOREOGRAFİ ile sürer: merkezi
// koordinatör yoktur; event log'a abone olur ve her trade event tipine
// KÜÇÜK, BAĞIMSIZ bir tepki verir. Saga, bu tepkilerin ürettiği yeni
// event'lerin tetiklediği zincirden belirir.
//
// Orkestrasyonla kıyas: akış tek yerde değil, tepki handler'larına
// dağılmıştır (onProposed, onAccepted, ...). Her handler yalnızca "şu
// event geldi, fazım uygunsa şu adımı at" der; kimsenin bütünü görmesi
// gerekmez — ama kimse de bütünü tek bakışta göremez.
//
// Handler'lar İDEMPOTENT'tir: her tepki, iş yapmadan önce trade
// stream'ini fold'layıp fazın beklenen değerde olduğunu doğrular. Böylece
// projection restart'ta event'leri baştan tekrar oynatsa bile adımlar iki
// kez uygulanmaz (envanter işlemleri de tradeID ile idempotenttir).
type Choreographer struct {
	eng  *engine
	stop chan struct{}
	done chan struct{}
}

// StartChoreographer, koreografi koordinatörünü başlatır (arka planda
// event akışını izler).
func StartChoreographer(store *es.Store) *Choreographer {
	c := &Choreographer{
		eng:  newEngine(store),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		defer close(c.done)
		es.Project(store, c.stop, c.apply)
	}()
	return c
}

func (c *Choreographer) Close() {
	close(c.stop)
	<-c.done
}

// ---- teklif ve dış girdiler (karşı taraf / süre sweeper'ı) ----

// Propose, takası başlatır: trade stream'ine Proposed yazar. Gerisi
// tepki zinciriyle akar.
func (c *Choreographer) Propose(o Offer) error { return c.eng.appendProposed(o) }

// Accept/Reject/Expire, karşı tarafın (veya süre sweeper'ının) girdisidir
// — koreografide bunlar birer EVENT'tir (orkestrasyonda ise Decider'ın
// dönüş değeri).
func (c *Choreographer) Accept(tradeID string) error {
	return c.eng.appendPhase(tradeID, EventAccepted)
}
func (c *Choreographer) Reject(tradeID string) error {
	return c.eng.appendPhase(tradeID, EventRejected)
}
func (c *Choreographer) Expire(tradeID string) error {
	return c.eng.appendPhase(tradeID, EventExpired)
}

// ---- tepki dağıtımı ----

func (c *Choreographer) apply(evs []es.Event) {
	for _, e := range evs {
		if !strings.HasPrefix(e.Stream, "trade-") {
			continue
		}
		id := strings.TrimPrefix(e.Stream, "trade-")
		switch e.Type {
		case EventProposed:
			c.onProposed(id)
		case EventAccepted:
			c.onAccepted(id)
		case EventCounterpartyReserved:
			c.onCounterpartyReserved(id)
		case EventRejected, EventExpired:
			c.onAbort(id, e.Type)
		}
	}
}

// state, guard'lar için trade stream'ini fold'lar.
func (c *Choreographer) state(id string) State {
	st, err := Status(c.eng.store, id)
	if err != nil {
		log.Printf("trade choreographer: read %s: %v", id, err)
	}
	return st
}

// onProposed: proposer'ın malını rezerve et.
func (c *Choreographer) onProposed(id string) {
	st := c.state(id)
	if st.Phase != PhaseProposed {
		return // zaten ilerlemiş (idempotentlik)
	}
	if err := c.eng.reserveProposer(st.Offer); err != nil {
		c.eng.appendCancelled(id, "proposer reserve: "+err.Error())
		return
	}
	c.eng.appendPhase(id, EventProposerReserved)
}

// onAccepted: karşı taraf kabul etti → onun malını rezerve et.
func (c *Choreographer) onAccepted(id string) {
	st := c.state(id)
	if st.Phase != PhaseProposerReserved {
		return
	}
	if err := c.eng.reserveCounterparty(st.Offer); err != nil {
		c.eng.releaseProposer(st.Offer) // TELAFİ
		c.eng.appendCancelled(id, "counterparty reserve: "+err.Error())
		return
	}
	c.eng.appendPhase(id, EventCounterpartyReserved)
}

// onCounterpartyReserved: iki taraf da rezerve → takas.
func (c *Choreographer) onCounterpartyReserved(id string) {
	st := c.state(id)
	if st.Phase != PhaseCounterpartyReserved {
		return
	}
	if err := c.eng.settle(st.Offer); err != nil {
		c.eng.releaseProposer(st.Offer)     // TELAFİ
		c.eng.releaseCounterparty(st.Offer) // TELAFİ
		c.eng.appendCancelled(id, "settle: "+err.Error())
		return
	}
	c.eng.appendPhase(id, EventSettled)
}

// onAbort: reddetme veya süre dolması → proposer rezervasyonunu geri al.
// (Yalnızca proposer rezerve edilmişken anlamlı; başka fazda no-op.)
func (c *Choreographer) onAbort(id, cause string) {
	st := c.state(id)
	if st.Phase != PhaseProposerReserved {
		return
	}
	c.eng.releaseProposer(st.Offer) // TELAFİ
	reason := "rejected by counterparty"
	if cause == EventExpired {
		reason = "timed out awaiting counterparty"
	}
	c.eng.appendCancelled(id, reason)
}
