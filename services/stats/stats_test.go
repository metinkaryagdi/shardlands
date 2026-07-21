package stats

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"shardlands/pkg/es"
	"shardlands/services/inventory"
)

func testStore(t *testing.T) (*es.Store, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-stats-*")
	if err != nil {
		t.Fatal(err)
	}
	s, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); os.RemoveAll(dir) })
	return s, dir
}

func gather(t *testing.T, s *es.Store, player, kind string, amount int) {
	t.Helper()
	data, _ := json.Marshal(inventory.Gathered{PlayerID: player, Kind: kind, Amount: amount})
	if _, err := s.Append(inventory.Stream(player), es.AnyVersion,
		es.EventData{Type: inventory.EventGathered, Data: data}); err != nil {
		t.Fatal(err)
	}
}

func waitTotal(t *testing.T, st *Stats, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st.TotalGathered() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("total never reached %d (have %d)", want, st.TotalGathered())
}

// Toplam toplanan: gather event'lerini sayar; ilgisiz event'leri (chat)
// yok sayar; catch-up + canlı akış.
func TestTotalGatheredCounts(t *testing.T) {
	s, _ := testStore(t)
	gather(t, s, "p1", "wood", 1) // projection başlamadan

	st := New(s, "world-0")
	defer st.Close()
	waitTotal(t, st, 1)

	gather(t, s, "p1", "crystal", 1)
	gather(t, s, "p2", "wood", 1)
	s.Append("chat", es.AnyVersion, es.EventData{Type: "ChatSaid", Data: []byte(`{}`)}) // gürültü
	waitTotal(t, st, 3)

	// Tek düğüm: G-Counter tek bileşenli olmalı.
	state := st.GatheredState()
	if len(state) != 1 || state["world-0"] != 3 {
		t.Fatalf("gcounter state = %v, want {world-0:3}", state)
	}
}

// Restart: projection log'dan sıfırdan kurulur; sayaç aynı toplama
// yakınsar (in-memory G-Counter persist edilmez, replay yeniden kurar).
func TestRestartRebuildsTotal(t *testing.T) {
	s, dir := testStore(t)
	for i := 0; i < 5; i++ {
		gather(t, s, "p1", "wood", 2)
	}
	st := New(s, "world-0")
	waitTotal(t, st, 10)
	st.Close()

	st2 := New(s, "world-0")
	defer st2.Close()
	waitTotal(t, st2, 10) // aynı toplam, sıfırdan
	_ = dir
}
