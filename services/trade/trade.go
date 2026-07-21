// Package trade, iki oyuncu arasındaki pazar yeri takasını SAGA olarak
// uygular. Aynı takas mantığı İKİ farklı koordinasyon stiliyle yazılıdır:
//
//   - choreographer.go: KOREOGRAFİ — merkezi koordinatör yok; bileşenler
//     event'lere tepki verir, saga tepki zincirinden belirir.
//   - orchestrator.go: ORKESTRASYON — tek koordinatör adımları açıkça
//     sürer, başarısızlıkta telafileri ters sırayla çalıştırır.
//
// İkisi de AYNI adım mekaniği (steps.go) üstünde çalışır; fark yalnızca
// koordinasyon. Karşılaştırma README'de.
//
// Saga akışı: teklif → A'nın malını rezerve et → B kabul eder → B'nin
// malını rezerve et → takas (çapraz commit + receive). Telafiler:
//   - A'nın malı yetmez            → iptal (rezerve edilecek şey yok)
//   - B reddeder / süre dolar       → A'nın rezervasyonunu geri al
//   - B'nin malı yetmez             → A'nın rezervasyonunu geri al
//   - takas (settle) başarısız      → HER İKİ rezervasyonu geri al
package trade

import (
	"encoding/json"

	"shardlands/pkg/es"
)

// Item, bir tür + miktar.
type Item struct {
	Kind   string `json:"kind"`
	Amount int    `json:"amount"`
}

// Offer, bir takas teklifidir: Proposer, Give'i verip Want'ı ister;
// Counterparty tersini yapar.
type Offer struct {
	ID           string `json:"id"`
	Proposer     string `json:"proposer"`
	Counterparty string `json:"counterparty"`
	Give         Item   `json:"give"` // proposer verir
	Want         Item   `json:"want"` // counterparty verir
}

// Trade stream event tipleri. Trade stream (trade-<id>) saga'nın kendi
// event log'u = durumudur; iki koordinasyon stili de ilerlemeyi buraya
// yazar. Accept/Reject/Expire DIŞ girdilerdir (karşı taraf veya süre
// sweeper'ı bunları ekler).
const (
	EventProposed             = "TradeProposed"
	EventProposerReserved     = "TradeProposerReserved"
	EventAccepted             = "TradeAccepted"
	EventRejected             = "TradeRejected"
	EventExpired              = "TradeExpired"
	EventCounterpartyReserved = "TradeCounterpartyReserved"
	EventSettled              = "TradeSettled"
	EventCancelled            = "TradeCancelled"
)

func Stream(id string) string { return "trade-" + id }

// Phase, saga'nın hangi aşamada olduğudur (trade stream'inden türetilir).
type Phase int

const (
	PhaseNone Phase = iota // teklif yok
	PhaseProposed
	PhaseProposerReserved
	PhaseCounterpartyReserved
	PhaseSettled   // terminal: başarı
	PhaseCancelled // terminal: telafi edildi
)

func (p Phase) Terminal() bool { return p == PhaseSettled || p == PhaseCancelled }

func (p Phase) String() string {
	switch p {
	case PhaseProposed:
		return "proposed"
	case PhaseProposerReserved:
		return "proposer-reserved"
	case PhaseCounterpartyReserved:
		return "counterparty-reserved"
	case PhaseSettled:
		return "settled"
	case PhaseCancelled:
		return "cancelled"
	default:
		return "none"
	}
}

// State, trade stream'inin foldudur.
type State struct {
	Offer  Offer
	Phase  Phase
	Reason string // yalnızca Cancelled'da dolu
}

// Fold, trade event'lerini duruma indirger. Hem sorgu (durum endpoint'i)
// hem de saga içi idempotentlik guard'ları bunu kullanır: bir tepki, yeni
// bir event yazmadan önce fazın beklenen değerde olduğunu doğrular; böylece
// event yeniden teslim edilse (restart) bile iş iki kez yapılmaz.
func Fold(events []es.Event) State {
	var st State
	for _, e := range events {
		switch e.Type {
		case EventProposed:
			json.Unmarshal(e.Data, &st.Offer)
			st.Phase = PhaseProposed
		case EventProposerReserved:
			st.Phase = PhaseProposerReserved
		case EventCounterpartyReserved:
			st.Phase = PhaseCounterpartyReserved
		case EventSettled:
			st.Phase = PhaseSettled
		case EventCancelled:
			st.Phase = PhaseCancelled
			var c struct {
				Reason string `json:"reason"`
			}
			if json.Unmarshal(e.Data, &c) == nil {
				st.Reason = c.Reason
			}
			// Accept/Reject/Expire fazı doğrudan değiştirmez; koordinatör
			// tepkisi onları CounterpartyReserved/Cancelled'a çevirir.
		}
	}
	return st
}

// Status, bir takasın güncel durumunu senkron okur (sorgu endpoint'i).
func Status(store *es.Store, tradeID string) (State, error) {
	evs, err := store.ReadStream(Stream(tradeID), 0, 0)
	if err != nil {
		return State{}, err
	}
	return Fold(evs), nil
}
