package trade

import (
	"errors"
	"fmt"

	"shardlands/pkg/es"
)

// Decision, karşı tarafın teklife yanıtıdır (orkestrasyon bunu senkron
// bekler; koreografide aynı bilgi Accept/Reject/Expire EVENT'i olur).
type Decision int

const (
	Accept Decision = iota
	Reject
	Timeout
)

// Decider, orkestratörün karşı tarafın kararını öğrenmek için
// çağırdığı fonksiyondur. Gerçekte karşı tarafın istemcisini bekler;
// testlerde stub, canlı sunucuda otomatik-kabul.
type Decider func(Offer) (Decision, error)

// Orchestrator, takas saga'sını MERKEZİ olarak sürer. Akışın tamamı tek
// fonksiyonda okunur; her başarısızlık noktasının telafisi hemen yanında.
type Orchestrator struct {
	eng *engine
}

func NewOrchestrator(store *es.Store) *Orchestrator {
	return &Orchestrator{eng: newEngine(store)}
}

// forceSettleError (yalnız test): takas adımında çökme simülasyonu için
// settle'ı hata döndürür yapar.
func (o *Orchestrator) forceSettleError(err error) {
	o.eng.settle = func(Offer) error { return err }
}

// Execute, teklifi baştan sona sürer ve son durumu döner. decide, karşı
// tarafın kararını verir. Fonksiyon boyunca telafi mantığının HEP ADIMIN
// YANINDA olduğuna dikkat: orkestrasyonun okunabilirlik kazancı budur.
func (o *Orchestrator) Execute(offer Offer, decide Decider) (State, error) {
	if err := o.eng.appendProposed(offer); err != nil {
		return State{}, err
	}

	// Adım 1: proposer'ın malını rezerve et.
	if err := o.eng.reserveProposer(offer); err != nil {
		return o.cancel(offer, "proposer reserve: "+err.Error())
	}
	if err := o.eng.appendPhase(offer.ID, EventProposerReserved); err != nil {
		return State{}, err
	}

	// Adım 2: karşı tarafın kararı.
	dec, err := decide(offer)
	switch {
	case err != nil:
		o.eng.releaseProposer(offer) // TELAFİ (adım 1'i geri sar)
		return o.cancel(offer, "decider error: "+err.Error())
	case dec == Reject:
		o.eng.releaseProposer(offer)
		return o.cancel(offer, "rejected by counterparty")
	case dec == Timeout:
		o.eng.releaseProposer(offer)
		return o.cancel(offer, "timed out awaiting counterparty")
	}

	// Adım 3: karşı tarafın malını rezerve et.
	if err := o.eng.reserveCounterparty(offer); err != nil {
		o.eng.releaseProposer(offer) // TELAFİ
		return o.cancel(offer, "counterparty reserve: "+err.Error())
	}
	if err := o.eng.appendPhase(offer.ID, EventCounterpartyReserved); err != nil {
		return State{}, err
	}

	// Adım 4: takas (çapraz transfer).
	if err := o.eng.settle(offer); err != nil {
		o.eng.releaseProposer(offer)     // TELAFİ (ikisini de geri sar)
		o.eng.releaseCounterparty(offer) // TELAFİ
		return o.cancel(offer, "settle: "+err.Error())
	}
	if err := o.eng.appendPhase(offer.ID, EventSettled); err != nil {
		return State{}, err
	}
	return State{Offer: offer, Phase: PhaseSettled}, nil
}

func (o *Orchestrator) cancel(offer Offer, reason string) (State, error) {
	if err := o.eng.appendCancelled(offer.ID, reason); err != nil {
		return State{}, err
	}
	return State{Offer: offer, Phase: PhaseCancelled, Reason: reason}, nil
}

// AutoAccept: karşı taraf her zaman kabul eder (canlı sunucu için basit
// decider; gerçek onay UX'i kapsam dışı).
func AutoAccept(Offer) (Decision, error) { return Accept, nil }

// ErrRejected/ErrTimeout: decider'ların standart nedenleri.
var (
	ErrRejected = errors.New("trade: rejected by counterparty")
	ErrTimeout  = fmt.Errorf("trade: counterparty timed out")
)
