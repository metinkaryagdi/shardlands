package trade

import (
	"encoding/json"

	"shardlands/pkg/es"
	"shardlands/services/inventory"
)

// engine, iki koordinasyon stilinin paylaştığı adım mekaniğidir. Böylece
// koreografi ile orkestrasyon arasındaki fark KOORDİNASYON stilidir, adım
// mekaniği değil — karşılaştırma dürüst olur.
//
// settle alanı testlerde geçici olarak değiştirilebilir (takas anında
// çökme senaryosu); nil ise gerçek çapraz-transfer kullanılır.
type engine struct {
	store  *es.Store
	settle func(Offer) error
}

func newEngine(store *es.Store) *engine {
	e := &engine{store: store}
	e.settle = e.crossTransfer
	return e
}

// reserveProposer / reserveCounterparty: ilgili tarafın malını tutar.
// Reserve optimistic concurrency'lidir; yetmezse hata döner.
func (e *engine) reserveProposer(o Offer) error {
	return inventory.Reserve(e.store, o.Proposer, o.ID, o.Give.Kind, o.Give.Amount)
}

func (e *engine) reserveCounterparty(o Offer) error {
	return inventory.Reserve(e.store, o.Counterparty, o.ID, o.Want.Kind, o.Want.Amount)
}

// crossTransfer, iki rezervasyonu çapraz taahhüt eder: A'nın verdiği
// B'ye, B'nin verdiği A'ya. Commit rezerveyi kalıcı düşürür, Receive
// karşı tarafa ekler.
func (e *engine) crossTransfer(o Offer) error {
	if err := inventory.Commit(e.store, o.Proposer, o.ID, o.Give.Kind, o.Give.Amount); err != nil {
		return err
	}
	if err := inventory.Receive(e.store, o.Counterparty, o.ID, o.Give.Kind, o.Give.Amount); err != nil {
		return err
	}
	if err := inventory.Commit(e.store, o.Counterparty, o.ID, o.Want.Kind, o.Want.Amount); err != nil {
		return err
	}
	return inventory.Receive(e.store, o.Proposer, o.ID, o.Want.Kind, o.Want.Amount)
}

// Telafiler: tutulan rezervasyonu geri ver.
func (e *engine) releaseProposer(o Offer) error {
	return inventory.Release(e.store, o.Proposer, o.ID, o.Give.Kind, o.Give.Amount)
}

func (e *engine) releaseCounterparty(o Offer) error {
	return inventory.Release(e.store, o.Counterparty, o.ID, o.Want.Kind, o.Want.Amount)
}

// ---- trade stream'e event yazma yardımcıları ----

// appendPhase, trade stream'ine faz event'i ekler. AnyVersion: trade
// stream'ine hem koordinatör hem dış girdiler (accept/reject) yazar;
// idempotentlik faz guard'larıyla sağlanır, versiyon kilidiyle değil.
func (e *engine) appendPhase(tradeID, typ string) error {
	_, err := e.store.Append(Stream(tradeID), es.AnyVersion, es.EventData{Type: typ})
	return err
}

func (e *engine) appendProposed(o Offer) error {
	data, _ := json.Marshal(o)
	_, err := e.store.Append(Stream(o.ID), es.AnyVersion,
		es.EventData{Type: EventProposed, Data: data})
	return err
}

func (e *engine) appendCancelled(tradeID, reason string) error {
	data, _ := json.Marshal(struct {
		Reason string `json:"reason"`
	}{reason})
	_, err := e.store.Append(Stream(tradeID), es.AnyVersion,
		es.EventData{Type: EventCancelled, Data: data})
	return err
}
