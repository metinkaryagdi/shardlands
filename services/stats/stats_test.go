package stats

import (
	"encoding/json"
	"testing"
	"time"

	"shardlands/internal/testenv"
	"shardlands/services/inventory"
)

func gather(t *testing.T, env *testenv.Env, player, kind string, amount int) {
	t.Helper()
	data, _ := json.Marshal(inventory.Gathered{PlayerID: player, Kind: kind, Amount: amount})
	env.Append(t, inventory.Stream(player), inventory.EventGathered, data)
}

func waitTotal(t *testing.T, st *Stats, want uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if st.TotalGathered() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("total never reached %d (have %d)", want, st.TotalGathered())
}

// Toplam toplanan: bus'tan gelen gather event'lerini sayar; ilgisiz
// event'leri yok sayar; abonelik öncesi yazılanları da (baştan oynatma)
// görür.
func TestTotalGatheredCounts(t *testing.T) {
	env := testenv.New(t)
	gather(t, env, "p1", "wood", 1) // projection başlamadan
	env.WaitDelivered(t)

	st, err := New(env.Bus, "world-0")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	waitTotal(t, st, 1)

	gather(t, env, "p1", "crystal", 1)
	gather(t, env, "p2", "wood", 1)
	env.Append(t, "chat", "ChatSaid", []byte(`{}`)) // gürültü
	waitTotal(t, st, 3)

	// Tek düğüm: G-Counter tek bileşenli olmalı.
	state := st.GatheredState()
	if len(state) != 1 || state["world-0"] != 3 {
		t.Fatalf("gcounter state = %v, want {world-0:3}", state)
	}
}

// Yeniden kurulum: yeni bir Stats örneği akışı baştan oynatarak aynı
// toplama ulaşır (in-memory read model'ler her açılışta yeniden kurulur).
func TestRebuildFromStreamReplay(t *testing.T) {
	env := testenv.New(t)
	for i := 0; i < 5; i++ {
		gather(t, env, "p1", "wood", 2)
	}
	env.WaitDelivered(t)

	st, err := New(env.Bus, "world-0")
	if err != nil {
		t.Fatal(err)
	}
	waitTotal(t, st, 10)
	st.Close()

	st2, err := New(env.Bus, "world-0")
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	waitTotal(t, st2, 10) // aynı toplam, sıfırdan
}
