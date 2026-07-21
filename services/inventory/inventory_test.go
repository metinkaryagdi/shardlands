package inventory

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"shardlands/pkg/es"
	"shardlands/services/world"
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

func gathered(t *testing.T, s *es.Store, playerID, kind string, amount int) {
	t.Helper()
	data, _ := json.Marshal(world.ResourceGathered{
		PlayerID: playerID, Name: playerID, NodeID: "n", Kind: kind, Amount: amount,
	})
	if _, err := s.Append(world.InvStream(playerID), es.AnyVersion,
		es.EventData{Type: world.EventResourceGathered, Data: data}); err != nil {
		t.Fatal(err)
	}
}

func waitCount(t *testing.T, inv *Inventory, playerID, kind string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if inv.Get(playerID)[kind] == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s/%s never reached %d (have %v)", playerID, kind, want, inv.Get(playerID))
}

// Catch-up + canlı akış; oyuncular ve türler ayrı sayılmalı; ilgisiz
// stream'ler (chat vs.) yok sayılmalı.
func TestInventoryCounts(t *testing.T) {
	s := testStore(t)
	gathered(t, s, "p-1", "wood", 1) // projection başlamadan önce

	inv := New(s)
	defer inv.Close()
	waitCount(t, inv, "p-1", "wood", 1)

	gathered(t, s, "p-1", "wood", 1)
	gathered(t, s, "p-1", "crystal", 1)
	gathered(t, s, "p-2", "wood", 1)
	s.Append(world.ChatStream, es.AnyVersion,
		es.EventData{Type: world.EventChatSaid, Data: []byte(`{}`)}) // gürültü

	waitCount(t, inv, "p-1", "wood", 2)
	waitCount(t, inv, "p-1", "crystal", 1)
	waitCount(t, inv, "p-2", "wood", 1)
	if got := inv.Get("p-2")["crystal"]; got != 0 {
		t.Fatalf("p-2 crystal = %d, want 0", got)
	}
	if got := inv.Get("hiç-yok"); len(got) != 0 {
		t.Fatalf("unknown player inventory = %v, want empty", got)
	}
}
