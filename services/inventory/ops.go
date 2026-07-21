package inventory

import (
	"encoding/json"
	"errors"
	"fmt"

	"shardlands/pkg/es"
)

// ErrInsufficient: oyuncunun available bakiyesi istenen miktardan az.
var ErrInsufficient = errors.New("inventory: insufficient balance")

// maxReserveRetries: optimistic concurrency çakışmasında kaç kez taze
// okuyup yeniden deneyeceğimiz. Sonlu; sonsuz çakışma (livelock) bir
// programlama hatasına işaret eder.
const maxReserveRetries = 8

// Bu işlemler tradeID + event tipi ile İDEMPOTENT'tir: aynı takasın aynı
// adımı iki kez çağrılırsa (saga tepkisinin restart'ta yeniden teslimi,
// veya "rezerve ettim ama işaretlemeden çöktüm" boşluğu) ikinci çağrı
// sessizce başarı döner. Idempotency key = tradeID; sagaların "at-least-
// once teslim" gerçeğiyle yaşamanın standart yolu.

// Reserve, oyuncunun kind türünden amount kadarını tradeID için tutar:
// stream'i yeniden kurar, available yeterli mi bakar ve OPTIMISTIC
// CONCURRENCY ile (beklenen versiyon = okuma anındaki versiyon) rezerve
// event'ini ekler. Araya başka bir yazma girdiyse Append çakışır; taze
// okuyup yeniden deneriz. Bu, AYRI takasların aynı malı aynı anda
// rezerve etmesini (çifte harcama) engeller.
func Reserve(store *es.Store, playerID, tradeID, kind string, amount int) error {
	if amount <= 0 {
		return fmt.Errorf("inventory: reserve amount must be positive, got %d", amount)
	}
	for attempt := 0; attempt < maxReserveRetries; attempt++ {
		evs, err := store.ReadStream(Stream(playerID), 0, 0)
		if err != nil {
			return err
		}
		if hasMove(evs, tradeID, EventReserved) {
			return nil // bu takas için zaten rezerve edilmiş (idempotent)
		}
		bal := Fold(evs)
		if bal.Available[kind] < amount {
			return fmt.Errorf("%w: %s has %d %s, need %d",
				ErrInsufficient, playerID, bal.Available[kind], kind, amount)
		}
		// ReadStream anlık tutarlı bir kopya döner; len(evs) o andaki
		// stream versiyonudur. Araya yazma girerse Append çakışır.
		expected := int64(len(evs))
		if _, err := appendMove(store, playerID, tradeID, kind, amount, EventReserved, expected); err != nil {
			if errors.Is(err, es.ErrVersionConflict) {
				continue // envanter değişti; taze bakiyeyle yeniden dene
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("inventory: reserve for %s exhausted retries (contention)", playerID)
}

// Release (telafi), tutulan rezervasyonu geri verir. Commit, rezerve
// malın kalıcı çıkışı. Receive, karşı taraftan gelen malın eklenmesi.
// Bunlar saga tarafından SERİLEŞTİRİLMİŞ çağrılır (aynı tradeID'yi tek
// koordinatör sürer); versiyon kilidi değil, tradeID idempotentliği
// yeterli.
func Release(store *es.Store, playerID, tradeID, kind string, amount int) error {
	return idempotentMove(store, playerID, tradeID, kind, amount, EventReleased)
}

func Commit(store *es.Store, playerID, tradeID, kind string, amount int) error {
	return idempotentMove(store, playerID, tradeID, kind, amount, EventCommitted)
}

func Receive(store *es.Store, playerID, tradeID, kind string, amount int) error {
	return idempotentMove(store, playerID, tradeID, kind, amount, EventReceived)
}

func idempotentMove(store *es.Store, playerID, tradeID, kind string, amount int, typ string) error {
	evs, err := store.ReadStream(Stream(playerID), 0, 0)
	if err != nil {
		return err
	}
	if hasMove(evs, tradeID, typ) {
		return nil // bu adım zaten uygulanmış (idempotent)
	}
	_, err = appendMove(store, playerID, tradeID, kind, amount, typ, es.AnyVersion)
	return err
}

// hasMove: stream'de tradeID'ye ait, typ tipinde bir hareket var mı?
func hasMove(events []es.Event, tradeID, typ string) bool {
	for _, e := range events {
		if e.Type != typ {
			continue
		}
		if m, ok := move(e); ok && m.TradeID == tradeID {
			return true
		}
	}
	return false
}

func appendMove(store *es.Store, playerID, tradeID, kind string, amount int, typ string, expected int64) ([]es.Event, error) {
	data, _ := json.Marshal(Move{PlayerID: playerID, TradeID: tradeID, Kind: kind, Amount: amount})
	return store.Append(Stream(playerID), expected, es.EventData{Type: typ, Data: data})
}
