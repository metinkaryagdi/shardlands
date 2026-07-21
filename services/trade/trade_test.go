package trade

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"shardlands/pkg/es"
	"shardlands/services/inventory"
)

func testStore(t *testing.T) *es.Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-trade-*")
	if err != nil {
		t.Fatal(err)
	}
	s, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); os.RemoveAll(dir) })
	return s
}

// give, oyuncuya kind türünden amount kadar kaynak verir (tek Gathered
// event'i) — takas testleri için başlangıç stoğu.
func give(t *testing.T, s *es.Store, playerID, kind string, amount int) {
	t.Helper()
	data, _ := json.Marshal(inventory.Gathered{
		PlayerID: playerID, Name: playerID, NodeID: "seed", Kind: kind, Amount: amount,
	})
	if _, err := s.Append(inventory.Stream(playerID), es.AnyVersion,
		es.EventData{Type: inventory.EventGathered, Data: data}); err != nil {
		t.Fatal(err)
	}
}

// avail, oyuncunun harcanabilir bakiyesini döner.
func avail(t *testing.T, s *es.Store, playerID string) map[string]int {
	t.Helper()
	evs, err := s.ReadStream(inventory.Stream(playerID), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return inventory.Fold(evs).Available
}

func reservedOf(t *testing.T, s *es.Store, playerID string) map[string]int {
	t.Helper()
	evs, _ := s.ReadStream(inventory.Stream(playerID), 0, 0)
	return inventory.Fold(evs).Reserved
}

func sampleOffer() Offer {
	return Offer{
		ID: "trade-1", Proposer: "p1", Counterparty: "p2",
		Give: Item{Kind: "wood", Amount: 2},
		Want: Item{Kind: "crystal", Amount: 3},
	}
}

func stock(t *testing.T, s *es.Store) {
	t.Helper()
	give(t, s, "p1", "wood", 5)
	give(t, s, "p2", "crystal", 5)
}

func waitTerminal(t *testing.T, s *es.Store, id string) State {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := Status(s, id)
		if st.Phase.Terminal() {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	st, _ := Status(s, id)
	t.Fatalf("trade %s not terminal in time (phase %s)", id, st.Phase)
	return State{}
}

// assertSettled: iki envanterin çapraz geçtiğini ve rezervasyon
// kalmadığını doğrular.
func assertSettled(t *testing.T, s *es.Store) {
	t.Helper()
	p1, p2 := avail(t, s, "p1"), avail(t, s, "p2")
	if p1["wood"] != 3 || p1["crystal"] != 3 {
		t.Fatalf("p1 = %v, want wood:3 crystal:3", p1)
	}
	if p2["wood"] != 2 || p2["crystal"] != 2 {
		t.Fatalf("p2 = %v, want wood:2 crystal:2", p2)
	}
	if len(reservedOf(t, s, "p1")) != 0 || len(reservedOf(t, s, "p2")) != 0 {
		t.Fatal("reservations remain after settle")
	}
}

// assertRolledBack: takas iptal oldu, KİMSENİN bakiyesi değişmedi,
// rezervasyon kalmadı (telafiler tam çalıştı).
func assertRolledBack(t *testing.T, s *es.Store, p1Wood, p2Crystal int) {
	t.Helper()
	if got := avail(t, s, "p1")["wood"]; got != p1Wood {
		t.Fatalf("p1 wood = %d, want %d (rollback)", got, p1Wood)
	}
	if got := avail(t, s, "p2")["crystal"]; got != p2Crystal {
		t.Fatalf("p2 crystal = %d, want %d (rollback)", got, p2Crystal)
	}
	if len(reservedOf(t, s, "p1")) != 0 || len(reservedOf(t, s, "p2")) != 0 {
		t.Fatalf("reservations remain after cancel: p1=%v p2=%v",
			reservedOf(t, s, "p1"), reservedOf(t, s, "p2"))
	}
}

// ======== ORKESTRASYON ========

func TestOrchestratorHappyPath(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	o := NewOrchestrator(s)
	st, err := o.Execute(sampleOffer(), AutoAccept)
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != PhaseSettled {
		t.Fatalf("phase = %s, want settled", st.Phase)
	}
	assertSettled(t, s)
}

func TestOrchestratorRejected(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	o := NewOrchestrator(s)
	reject := func(Offer) (Decision, error) { return Reject, nil }
	st, _ := o.Execute(sampleOffer(), reject)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 5, 5) // hiç mal el değiştirmedi
}

func TestOrchestratorTimeout(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	o := NewOrchestrator(s)
	timeout := func(Offer) (Decision, error) { return Timeout, nil }
	st, _ := o.Execute(sampleOffer(), timeout)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 5, 5)
}

func TestOrchestratorProposerInsufficient(t *testing.T) {
	s := testStore(t)
	give(t, s, "p2", "crystal", 5) // p1'in hiç odunu yok
	o := NewOrchestrator(s)
	st, _ := o.Execute(sampleOffer(), AutoAccept)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 0, 5)
}

func TestOrchestratorCounterpartyInsufficient(t *testing.T) {
	s := testStore(t)
	give(t, s, "p1", "wood", 5) // p2'nin hiç kristali yok
	o := NewOrchestrator(s)
	st, _ := o.Execute(sampleOffer(), AutoAccept)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	// p1 rezerve edilmişti, telafiyle geri alınmalı.
	assertRolledBack(t, s, 5, 0)
}

func TestOrchestratorSettleFailureCompensatesBoth(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	o := NewOrchestrator(s)
	o.forceSettleError(errors.New("boom"))
	st, _ := o.Execute(sampleOffer(), AutoAccept)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	// İki taraf da rezerve edilmişti; ikisi de geri alınmalı.
	assertRolledBack(t, s, 5, 5)
}

// ======== KOREOGRAFİ ========

func TestChoreographyHappyPath(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	c := StartChoreographer(s)
	defer c.Close()

	off := sampleOffer()
	if err := c.Propose(off); err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(off.ID); err != nil {
		t.Fatal(err)
	}
	st := waitTerminal(t, s, off.ID)
	if st.Phase != PhaseSettled {
		t.Fatalf("phase = %s (%s), want settled", st.Phase, st.Reason)
	}
	assertSettled(t, s)
}

func TestChoreographyRejected(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	c := StartChoreographer(s)
	defer c.Close()

	off := sampleOffer()
	c.Propose(off)
	// Proposer rezerve edilene kadar bekle, sonra reddet (gerçekçi sıra:
	// karşı taraf teklifi kurulduktan sonra reddeder).
	waitPhaseAtLeast(t, s, off.ID, PhaseProposerReserved)
	c.Reject(off.ID)

	st := waitTerminal(t, s, off.ID)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 5, 5)
}

func TestChoreographyTimeout(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	c := StartChoreographer(s)
	defer c.Close()

	off := sampleOffer()
	c.Propose(off)
	waitPhaseAtLeast(t, s, off.ID, PhaseProposerReserved)
	c.Expire(off.ID) // süre sweeper'ı gibi

	st := waitTerminal(t, s, off.ID)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 5, 5)
}

func TestChoreographyCounterpartyInsufficient(t *testing.T) {
	s := testStore(t)
	give(t, s, "p1", "wood", 5) // p2 kristalsiz
	c := StartChoreographer(s)
	defer c.Close()

	off := sampleOffer()
	c.Propose(off)
	waitPhaseAtLeast(t, s, off.ID, PhaseProposerReserved)
	c.Accept(off.ID)

	st := waitTerminal(t, s, off.ID)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 5, 0) // p1'in rezervasyonu telafiyle döndü
}

func TestChoreographySettleFailureCompensatesBoth(t *testing.T) {
	s := testStore(t)
	stock(t, s)
	c := StartChoreographer(s)
	c.eng.settle = func(Offer) error { return errors.New("boom") }
	defer c.Close()

	off := sampleOffer()
	c.Propose(off)
	c.Accept(off.ID)
	st := waitTerminal(t, s, off.ID)
	if st.Phase != PhaseCancelled {
		t.Fatalf("phase = %s, want cancelled", st.Phase)
	}
	assertRolledBack(t, s, 5, 5)
}

// Restart idempotentliği: saga ortasında koordinatör ölür; yeni
// koordinatör event'leri BAŞTAN oynatır. Adımlar tradeID ile idempotent
// olduğu için ikinci kez uygulanmaz — sonuç bir kez settle olmuş gibi.
func TestChoreographyRestartIdempotent(t *testing.T) {
	s := testStore(t)
	stock(t, s)

	c1 := StartChoreographer(s)
	off := sampleOffer()
	c1.Propose(off)
	c1.Accept(off.ID)
	waitTerminal(t, s, off.ID)
	assertSettled(t, s)
	c1.Close()

	// Envanter stream uzunluklarını kaydet.
	p1Before, _ := s.ReadStream(inventory.Stream("p1"), 0, 0)
	p2Before, _ := s.ReadStream(inventory.Stream("p2"), 0, 0)

	// Yeni koordinatör: tüm event'leri checkpoint 0'dan yeniden oynatır.
	c2 := StartChoreographer(s)
	defer c2.Close()
	// Biraz zaman tanı ki tüm event'ler yeniden işlensin.
	time.Sleep(200 * time.Millisecond)

	p1After, _ := s.ReadStream(inventory.Stream("p1"), 0, 0)
	p2After, _ := s.ReadStream(inventory.Stream("p2"), 0, 0)
	if len(p1After) != len(p1Before) || len(p2After) != len(p2Before) {
		t.Fatalf("replay appended new events: p1 %d->%d, p2 %d->%d (not idempotent)",
			len(p1Before), len(p1After), len(p2Before), len(p2After))
	}
	assertSettled(t, s) // bakiye hâlâ tutarlı
}

// waitPhaseAtLeast, faz >= want olana kadar bekler.
func waitPhaseAtLeast(t *testing.T, s *es.Store, id string, want Phase) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := Status(s, id)
		if st.Phase >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("trade %s never reached phase %s", id, want)
}
