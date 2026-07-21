package world_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/services/inventory"
	"shardlands/services/world"
)

// collector: Snapshot'ları kanala akıtan sahte oturum aktörü.
func collector(t *testing.T, sys *actor.System, name string) (*actor.Ref, chan world.Snapshot) {
	t.Helper()
	ch := make(chan world.Snapshot, 64)
	ref, err := sys.Spawn(actor.Props{
		Name: name,
		Producer: func() actor.Actor {
			return actor.ReceiverFunc(func(ctx *actor.Context) {
				if s, ok := ctx.Message().(world.Snapshot); ok {
					ch <- s
				}
			})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref, ch
}

func nextSnap(t *testing.T, ch chan world.Snapshot) world.Snapshot {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot received")
		return world.Snapshot{}
	}
}

// Tick'ler elle enjekte edilir: simülasyon tamamen deterministik.
func TestMovementAndBroadcast(t *testing.T) {
	sys := actor.NewSystem("test")
	defer sys.Shutdown()

	w, err := sys.Spawn(world.Props(nil))
	if err != nil {
		t.Fatal(err)
	}
	s1, ch1 := collector(t, sys, "sess1")
	s2, ch2 := collector(t, sys, "sess2")

	w.Send(world.Join{PlayerID: "p1", Name: "ayşe", Session: s1})
	w.Send(world.Join{PlayerID: "p2", Name: "bora", Session: s2})
	w.Send(world.Input{PlayerID: "p1", Right: true})
	w.Send(world.Tick{})

	const step = world.Speed / world.TickRate // tick başına px

	snap := nextSnap(t, ch1)
	if len(snap.Players) != 2 {
		t.Fatalf("players = %d, want 2", len(snap.Players))
	}
	// Sıralama ID'ye göre deterministik: p1, p2.
	p1, p2 := snap.Players[0], snap.Players[1]
	if p1.ID != "p1" || p2.ID != "p2" {
		t.Fatalf("order = %s,%s want p1,p2", p1.ID, p2.ID)
	}
	if p1.X != world.Width/2+step || p1.Y != world.Height/2 {
		t.Fatalf("p1 = (%v,%v), want (%v,%v)", p1.X, p1.Y, world.Width/2+step, world.Height/2)
	}
	if p2.X != world.Width/2 {
		t.Fatalf("p2 moved without input: x=%v", p2.X)
	}
	// İkinci oturum da aynı kareyi almalı.
	if got := nextSnap(t, ch2); got.Tick != snap.Tick {
		t.Fatalf("sessions saw different ticks: %d vs %d", got.Tick, snap.Tick)
	}

	// Girdi durumu KALICI: yeni input gelmeden ikinci tick de sağa götürür.
	w.Send(world.Tick{})
	if got := nextSnap(t, ch1); got.Players[0].X != world.Width/2+2*step {
		t.Fatalf("second tick x = %v, want %v", got.Players[0].X, world.Width/2+2*step)
	}

	// Durdurma: boş input.
	w.Send(world.Input{PlayerID: "p1"})
	w.Send(world.Tick{})
	if got := nextSnap(t, ch1); got.Players[0].X != world.Width/2+2*step {
		t.Fatalf("after stop x = %v, want unchanged", got.Players[0].X)
	}
}

func TestLeaveRemovesPlayer(t *testing.T) {
	sys := actor.NewSystem("test")
	defer sys.Shutdown()

	w, _ := sys.Spawn(world.Props(nil))
	s1, ch1 := collector(t, sys, "sess1")
	s2, _ := collector(t, sys, "sess2")

	w.Send(world.Join{PlayerID: "p1", Name: "a", Session: s1})
	w.Send(world.Join{PlayerID: "p2", Name: "b", Session: s2})
	w.Send(world.Leave{PlayerID: "p2"})
	w.Send(world.Leave{PlayerID: "p2"}) // idempotent: ikinci Leave zararsız
	w.Send(world.Tick{})

	if snap := nextSnap(t, ch1); len(snap.Players) != 1 || snap.Players[0].ID != "p1" {
		t.Fatalf("players = %+v, want only p1", snap.Players)
	}
}

// Chat: geçerli komut balon üretir + event basar; balon süre dolunca
// silinir; geçersiz komut (boş/uzun) ikisini de yapmaz.
func TestChatBubbleAndEvent(t *testing.T) {
	dir, err := os.MkdirTemp("", "shardlands-world-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	store, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	sys := actor.NewSystem("test")
	defer sys.Shutdown()
	w, err := sys.Spawn(world.Props(store))
	if err != nil {
		t.Fatal(err)
	}
	s1, ch1 := collector(t, sys, "sess1")

	w.Send(world.Join{PlayerID: "p1", Name: "ayşe", Session: s1})
	w.Send(world.Chat{PlayerID: "p1", Text: "  selam dünya  "})
	w.Send(world.Tick{})

	snap := nextSnap(t, ch1)
	if got := snap.Players[0].Bubble; got != "selam dünya" {
		t.Fatalf("bubble = %q, want %q (trimmed)", got, "selam dünya")
	}

	// Event mağazaya düşmüş olmalı (aktör append'i senkron yapar).
	evs, err := store.ReadStream(world.ChatStream, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Type != world.EventChatSaid {
		t.Fatalf("chat events = %+v, want 1 ChatSaid", evs)
	}
	var said world.ChatSaid
	if err := json.Unmarshal(evs[0].Data, &said); err != nil {
		t.Fatal(err)
	}
	if said.PlayerID != "p1" || said.Name != "ayşe" || said.Text != "selam dünya" {
		t.Fatalf("event data = %+v", said)
	}

	// Balon ~4 saniye (bubbleTicks) sonra silinmeli. Tick gönder/oku iç
	// içe (boru hattı tamponlarını taşırmamak için — bkz. gather testi).
	var last world.Snapshot
	for i := 0; i < 4*world.TickRate; i++ {
		w.Send(world.Tick{})
		last = nextSnap(t, ch1)
	}
	if last.Players[0].Bubble != "" {
		t.Fatalf("bubble not expired: %q", last.Players[0].Bubble)
	}

	// Geçersiz komutlar: boş ve fazla uzun metin event üretmemeli.
	w.Send(world.Chat{PlayerID: "p1", Text: "   "})
	w.Send(world.Chat{PlayerID: "p1", Text: strings.Repeat("x", 121)})
	w.Send(world.Chat{PlayerID: "yok", Text: "hayalet"})
	w.Send(world.Tick{})
	if snap := nextSnap(t, ch1); snap.Players[0].Bubble != "" {
		t.Fatalf("invalid chat produced bubble: %q", snap.Players[0].Bubble)
	}
	if evs, _ := store.ReadStream(world.ChatStream, 0, 0); len(evs) != 1 {
		t.Fatalf("invalid chats appended events: %d, want still 1", len(evs))
	}
}

// Toplama: menzil kontrolü, node tüketimi, event basımı ve respawn.
func TestGatherDepleteAndRespawn(t *testing.T) {
	dir, err := os.MkdirTemp("", "shardlands-world-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	store, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	sys := actor.NewSystem("test")
	defer sys.Shutdown()
	w, err := sys.Spawn(world.Props(store))
	if err != nil {
		t.Fatal(err)
	}
	s1, ch1 := collector(t, sys, "sess1")
	w.Send(world.Join{PlayerID: "p1", Name: "ayşe", Session: s1})

	// Merkezden (400,300) uzaktaki node menzil dışı: toplama sessizce
	// başarısız olmalı (n5 (400,180) 120px uzakta, menzil 48).
	w.Send(world.Gather{PlayerID: "p1"})
	if evs, _ := store.ReadStream(inventory.Stream("p1"), 0, 0); len(evs) != 0 {
		t.Fatalf("out-of-range gather appended %d events", len(evs))
	}

	// n5'e yürü: yukarı 12 tick = 120px → (400,180). Tick gönder/oku
	// İÇ İÇE: toplu göndermek world→collector→kanal boru hattının
	// tamponlarını (64+64+64) aşınca deadlock olur — Block mailbox'lı
	// aktör zincirlerinde backpressure gerçek bir şeydir.
	w.Send(world.Input{PlayerID: "p1", Up: true})
	for i := 0; i < 12; i++ {
		w.Send(world.Tick{})
		nextSnap(t, ch1)
	}
	w.Send(world.Input{PlayerID: "p1"}) // dur

	w.Send(world.Gather{PlayerID: "p1"})
	w.Send(world.Tick{})
	snap := nextSnap(t, ch1)
	var n5 world.NodeState
	for _, n := range snap.Nodes {
		if n.ID == "n5" {
			n5 = n
		}
	}
	if n5.Available {
		t.Fatal("gathered node must be depleted in snapshot")
	}

	evs, err := store.ReadStream(inventory.Stream("p1"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Type != inventory.EventGathered {
		t.Fatalf("inv events = %+v, want 1 ResourceGathered", evs)
	}
	var g inventory.Gathered
	json.Unmarshal(evs[0].Data, &g)
	if g.NodeID != "n5" || g.Kind != "wood" || g.Amount != 1 || g.PlayerID != "p1" {
		t.Fatalf("event data = %+v", g)
	}

	// Tükenmiş node tekrar toplanamaz.
	w.Send(world.Gather{PlayerID: "p1"})
	if evs, _ := store.ReadStream(inventory.Stream("p1"), 0, 0); len(evs) != 1 {
		t.Fatalf("depleted node re-gathered: %d events", len(evs))
	}

	// Respawn: RespawnTicks sonra node yeniden müsait ve toplanabilir.
	var last world.Snapshot
	for i := 0; i < world.RespawnTicks+1; i++ {
		w.Send(world.Tick{})
		last = nextSnap(t, ch1)
	}
	for _, n := range last.Nodes {
		if n.ID == "n5" && !n.Available {
			t.Fatal("node did not respawn")
		}
	}
	w.Send(world.Gather{PlayerID: "p1"})
	w.Send(world.Tick{}) // aktörün Gather'ı işlediğinden emin ol (senkron noktası)
	nextSnap(t, ch1)
	if evs, _ := store.ReadStream(inventory.Stream("p1"), 0, 0); len(evs) != 2 {
		t.Fatalf("respawned node not gatherable: %d events, want 2", len(evs))
	}
}

// Dünya sınırları: sola koşmaya devam eden oyuncu 0'da durmalı.
func TestClampAtBounds(t *testing.T) {
	sys := actor.NewSystem("test")
	defer sys.Shutdown()

	w, _ := sys.Spawn(world.Props(nil))
	s1, ch1 := collector(t, sys, "sess1")
	w.Send(world.Join{PlayerID: "p1", Name: "a", Session: s1})
	w.Send(world.Input{PlayerID: "p1", Left: true})

	// Merkezden sol kenara yeter de artar sayıda tick (gönder/oku iç içe).
	needed := int(world.Width/2/(world.Speed/world.TickRate)) + 5
	var last world.Snapshot
	for i := 0; i < needed; i++ {
		w.Send(world.Tick{})
		last = nextSnap(t, ch1)
	}
	if last.Players[0].X != 0 {
		t.Fatalf("x = %v, want clamped at 0", last.Players[0].X)
	}
}
