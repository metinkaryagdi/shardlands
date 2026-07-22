package inventory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"shardlands/internal/testenv"
	"shardlands/pkg/es"
)

func testStore(t *testing.T) *es.Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-inv-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func gather(t *testing.T, s *es.Store, playerID, kind string, amount int) {
	t.Helper()
	data, _ := json.Marshal(Gathered{PlayerID: playerID, Name: playerID, NodeID: "n", Kind: kind, Amount: amount})
	if _, err := s.Append(Stream(playerID), es.AnyVersion,
		es.EventData{Type: EventGathered, Data: data}); err != nil {
		t.Fatal(err)
	}
}

func waitAvail(t *testing.T, inv *Inventory, playerID, kind string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if inv.Get(playerID)[kind] == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s/%s never reached %d (have %v)", playerID, kind, want, inv.Get(playerID))
}

// Read model artık BUS'tan tüketiyor: baştan oynatma (catch-up) + canlı
// akış; oyuncular/türler ayrı; ilgisiz stream'ler yok sayılır.
func TestReadModelCounts(t *testing.T) {
	env := testenv.New(t)
	gather(t, env.Store, "p-1", "wood", 1) // projection başlamadan
	env.WaitDelivered(t)

	inv, err := New(env.Bus)
	if err != nil {
		t.Fatal(err)
	}
	defer inv.Close()
	waitAvail(t, inv, "p-1", "wood", 1)

	gather(t, env.Store, "p-1", "wood", 1)
	gather(t, env.Store, "p-1", "crystal", 1)
	gather(t, env.Store, "p-2", "wood", 1)
	env.Store.Append("chat", es.AnyVersion, es.EventData{Type: "ChatSaid", Data: []byte(`{}`)}) // gürültü

	waitAvail(t, inv, "p-1", "wood", 2)
	waitAvail(t, inv, "p-1", "crystal", 1)
	waitAvail(t, inv, "p-2", "wood", 1)
	if got := inv.Get("hiç-yok"); len(got) != 0 {
		t.Fatalf("unknown player = %v, want empty", got)
	}
}

// Fold: rezervasyon available'ı düşürür, reserved'a taşır; release geri
// verir; commit rezerveyi kalıcı düşürür; receive available ekler.
func TestFoldBalanceTransitions(t *testing.T) {
	s := testStore(t)
	gather(t, s, "p-1", "wood", 5)

	if err := Reserve(s, "p-1", "t1", "wood", 3); err != nil {
		t.Fatal(err)
	}
	evs, _ := s.ReadStream(Stream("p-1"), 0, 0)
	b := Fold(evs)
	if b.Available["wood"] != 2 || b.Reserved["wood"] != 3 {
		t.Fatalf("after reserve: avail=%d reserved=%d, want 2/3", b.Available["wood"], b.Reserved["wood"])
	}

	// Yetersiz bakiye: 3 kaldı, 3 rezerve edilemez (available 2).
	if err := Reserve(s, "p-1", "t2", "wood", 3); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("over-reserve = %v, want ErrInsufficient", err)
	}

	if err := Release(s, "p-1", "t1", "wood", 3); err != nil {
		t.Fatal(err)
	}
	evs, _ = s.ReadStream(Stream("p-1"), 0, 0)
	b = Fold(evs)
	if b.Available["wood"] != 5 || b.Reserved["wood"] != 0 {
		t.Fatalf("after release: avail=%d reserved=%d, want 5/0", b.Available["wood"], b.Reserved["wood"])
	}

	// Commit + Receive: rezerve et, taahhüt et (çıkış), karşı taraf gibi al.
	Reserve(s, "p-1", "t3", "wood", 2)
	Commit(s, "p-1", "t3", "wood", 2)
	Receive(s, "p-1", "t3", "crystal", 1)
	evs, _ = s.ReadStream(Stream("p-1"), 0, 0)
	b = Fold(evs)
	if b.Available["wood"] != 3 || b.Reserved["wood"] != 0 || b.Available["crystal"] != 1 {
		t.Fatalf("after commit/receive: wood avail=%d res=%d crystal=%d, want 3/0/1",
			b.Available["wood"], b.Reserved["wood"], b.Available["crystal"])
	}
}

// Optimistic concurrency: aynı mala iki eşzamanlı rezervasyon; toplam
// rezerve, mevcut bakiyeyi AŞMAMALI (çifte harcama engellenmeli).
func TestConcurrentReserveNoDoubleSpend(t *testing.T) {
	s := testStore(t)
	gather(t, s, "p-1", "wood", 10)

	// 15 goroutine, her biri AYRI takas için 1 rezerve etmeye çalışır;
	// en fazla 10 başarır (aynı tradeID olsaydı idempotentlik hepsini
	// başarı sayardı — çifte harcama testi ayrı takaslar ister).
	const workers = 15
	results := make(chan error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func(i int) {
			<-start
			results <- Reserve(s, "p-1", fmt.Sprintf("t%d", i), "wood", 1)
		}(i)
	}
	close(start)

	success := 0
	for i := 0; i < workers; i++ {
		if err := <-results; err == nil {
			success++
		} else if !errors.Is(err, ErrInsufficient) {
			t.Fatalf("unexpected reserve error: %v", err)
		}
	}
	if success != 10 {
		t.Fatalf("successful reserves = %d, want exactly 10 (no double-spend)", success)
	}
	evs, _ := s.ReadStream(Stream("p-1"), 0, 0)
	b := Fold(evs)
	if b.Available["wood"] != 0 || b.Reserved["wood"] != 10 {
		t.Fatalf("final: avail=%d reserved=%d, want 0/10", b.Available["wood"], b.Reserved["wood"])
	}
}
