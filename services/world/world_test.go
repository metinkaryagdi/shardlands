package world_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/services/inventory"
	"shardlands/services/world"
)

// collector, bir oturumu taklit eder: snapshot ve handoff (AssignedRegion)
// mesajlarını tampon kanallara akıtır. Bölge aktörünü ASLA bloklamaz
// (dolu kanalda düşürür) — DropNewest oturum davranışının test karşılığı.
type collector struct {
	snaps   chan world.Snapshot
	assigns chan world.AssignedRegion
}

func (c *collector) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case world.Snapshot:
		select {
		case c.snaps <- m:
		default:
		}
	case world.AssignedRegion:
		select {
		case c.assigns <- m:
		default:
		}
	}
}

type harness struct {
	t      *testing.T
	sys    *actor.System
	router *world.Router
}

func newHarness(t *testing.T, store *es.Store, shards ...string) *harness {
	t.Helper()
	if len(shards) == 0 {
		shards = []string{"shard-0", "shard-1"}
	}
	sys := actor.NewSystem("test")
	router, err := world.NewHub(sys, store, shards)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sys.Shutdown)
	return &harness{t: t, sys: sys, router: router}
}

func (h *harness) tickAll() {
	for _, ref := range h.router.Refs() {
		ref.Send(world.Tick{})
	}
}

// spawnPlayer, oyuncuyu (x,y)'de doğurur ve collector'ını döner; başlangıç
// bölge ref'i de döner.
func (h *harness) spawnPlayer(id, name string, x, y float64) (*collector, *actor.Ref) {
	h.t.Helper()
	c := &collector{snaps: make(chan world.Snapshot, 1024), assigns: make(chan world.AssignedRegion, 16)}
	sess, err := h.sys.Spawn(actor.Props{Producer: func() actor.Actor { return c }})
	if err != nil {
		h.t.Fatal(err)
	}
	_, _, ref := h.router.SpawnRegion(x, y)
	ref.Send(world.Join{PlayerID: id, Name: name, Session: sess, X: x, Y: y})
	return c, ref
}

func drainSnap(c *collector) (world.Snapshot, bool) {
	select {
	case s := <-c.snaps:
		return s, true
	default:
		return world.Snapshot{}, false
	}
}

// İki oyuncu aynı bölgede birbirini görmeli.
func TestCoRegionVisibility(t *testing.T) {
	h := newHarness(t, nil)
	cx, cy := world.Width/2, world.Height/2
	c1, _ := h.spawnPlayer("p1", "ayşe", cx, cy)
	c2, _ := h.spawnPlayer("p2", "bora", cx, cy)

	// Birkaç tick sonra ikisi de aynı bölgede iki oyuncu görmeli.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.tickAll()
		time.Sleep(5 * time.Millisecond)
		s, ok := lastSnap(c1)
		if ok && len(s.Players) == 2 {
			s2, ok2 := lastSnap(c2)
			if ok2 && len(s2.Players) == 2 && s2.RegionID == s.RegionID {
				return
			}
		}
	}
	t.Fatal("co-region players never saw each other")
}

// lastSnap, kanaldaki en yeni snapshot'ı döner (birikenleri atlar).
func lastSnap(c *collector) (world.Snapshot, bool) {
	var last world.Snapshot
	got := false
	for {
		s, ok := drainSnap(c)
		if !ok {
			return last, got
		}
		last, got = s, true
	}
}

// Handoff: oyuncu sol sınırı geçince bölge değişir; oturum AssignedRegion
// alır, kaynak bölgeden kaybolur, hedef bölgede görünür.
func TestHandoffAcrossBoundary(t *testing.T) {
	h := newHarness(t, nil)
	cx, cy := world.Width/2, world.Height/2 // (400,300) → r-1-1
	c, cur := h.spawnPlayer("p1", "gezgin", cx, cy)

	startRegion := world.RegionAt(cx, cy)
	if startRegion != "r-1-1" {
		t.Fatalf("spawn region = %s, want r-1-1", startRegion)
	}

	var assigned *world.AssignedRegion
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && assigned == nil {
		cur.Send(world.Input{PlayerID: "p1", Left: true}) // sola bas
		h.tickAll()
		time.Sleep(5 * time.Millisecond)
		select {
		case a := <-c.assigns:
			assigned = &a
			cur = a.Ref
		default:
		}
	}
	if assigned == nil {
		t.Fatal("no handoff after crossing left boundary")
	}
	if assigned.RegionID != "r-0-1" {
		t.Fatalf("handed off to %s, want r-0-1", assigned.RegionID)
	}

	// Hedef bölge snapshot'ında oyuncu görünmeli.
	found := false
	for time.Now().Before(deadline) && !found {
		cur.Send(world.Input{PlayerID: "p1"}) // dur
		h.tickAll()
		time.Sleep(5 * time.Millisecond)
		if s, ok := lastSnap(c); ok && s.RegionID == "r-0-1" {
			for _, p := range s.Players {
				if p.ID == "p1" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("player not visible in destination region after handoff")
	}
}

// Bölge→shard eşlemesi: her bölge bir shard'a atanır; RegionAt sınırları
// doğru; iki shard mevcut.
func TestRegionShardMapping(t *testing.T) {
	h := newHarness(t, nil, "shard-0", "shard-1")
	regions := []string{"r-0-0", "r-1-0", "r-0-1", "r-1-1"}
	shardsSeen := map[string]bool{}
	for _, rid := range regions {
		s := h.router.ShardOf(rid)
		if s == "" {
			t.Fatalf("region %s has no shard", rid)
		}
		shardsSeen[s] = true
		if h.router.Ref(rid) == nil {
			t.Fatalf("region %s has no actor", rid)
		}
	}
	// İki shard da en az bir bölge almalı (denge; 128 vnode ile beklenir).
	if len(shardsSeen) != 2 {
		t.Fatalf("regions spread over %d shards, want 2", len(shardsSeen))
	}
	// RegionAt sınır kontrolleri.
	cases := []struct {
		x, y float64
		want string
	}{
		{0, 0, "r-0-0"}, {399, 299, "r-0-0"}, {400, 300, "r-1-1"},
		{799, 599, "r-1-1"}, {200, 400, "r-0-1"}, {600, 100, "r-1-0"},
	}
	for _, c := range cases {
		if got := world.RegionAt(c.x, c.y); got != c.want {
			t.Fatalf("RegionAt(%.0f,%.0f) = %s, want %s", c.x, c.y, got, c.want)
		}
	}
}

// İzole shard (CAP önizleme): hedef bölgenin shard'ı down ise oyuncu
// sınırı geçemez, mevcut bölgede kalır.
func TestUnavailableShardBlocksHandoff(t *testing.T) {
	h := newHarness(t, nil)
	cx, cy := world.Width/2, world.Height/2
	c, cur := h.spawnPlayer("p1", "gezgin", cx, cy)

	// Sol komşu bölge r-0-1'in shard'ını indir.
	destShard := h.router.ShardOf("r-0-1")
	srcShard := h.router.ShardOf("r-1-1")
	if destShard == srcShard {
		t.Skip("komşu bölgeler aynı shard'ta; bu kurulumda anlamlı değil")
	}
	h.router.SetShardUp(destShard, false)

	// Sola bas; handoff OLMAMALI, oyuncu r-1-1'de kalmalı (sınıra sıkışır).
	sawAssign := false
	for i := 0; i < 60; i++ {
		cur.Send(world.Input{PlayerID: "p1", Left: true})
		h.tickAll()
		time.Sleep(5 * time.Millisecond)
		select {
		case <-c.assigns:
			sawAssign = true
		default:
		}
	}
	if sawAssign {
		t.Fatal("handoff happened despite destination shard being down (CAP: must block)")
	}
	if s, ok := lastSnap(c); ok {
		if s.RegionID != "r-1-1" {
			t.Fatalf("player left region %s despite down shard, want r-1-1", s.RegionID)
		}
		for _, p := range s.Players {
			if p.ID == "p1" && p.X >= world.RegionW {
				// hâlâ sağ yarıda (sınırın sağında) olmalı
				continue
			}
			if p.ID == "p1" && p.X < world.RegionW-2 {
				t.Fatalf("player x=%.1f crossed boundary despite down shard", p.X)
			}
		}
	}
}

// Toplama bölgede çalışır ve event yazar (store'lu).
func TestGatherInRegion(t *testing.T) {
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

	h := newHarness(t, store)
	// n6 kristal (400,420) r-1-1'de. Oyuncuyu oraya yakın doğur.
	c, cur := h.spawnPlayer("p1", "toplayıcı", 400, 420)
	_ = c
	cur.Send(world.Gather{PlayerID: "p1"})
	h.tickAll()
	time.Sleep(20 * time.Millisecond)

	evs, err := store.ReadStream(inventory.Stream("p1"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Type != inventory.EventGathered {
		t.Fatalf("gather events = %d, want 1", len(evs))
	}
	var g inventory.Gathered
	json.Unmarshal(evs[0].Data, &g)
	if g.Kind != "crystal" || g.NodeID != "n6" {
		t.Fatalf("gathered %+v, want crystal/n6", g)
	}
}
