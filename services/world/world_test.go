package world_test

import (
	"testing"
	"time"

	"shardlands/pkg/actor"
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

	w, err := sys.Spawn(world.Props())
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

	w, _ := sys.Spawn(world.Props())
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

// Dünya sınırları: sola koşmaya devam eden oyuncu 0'da durmalı.
func TestClampAtBounds(t *testing.T) {
	sys := actor.NewSystem("test")
	defer sys.Shutdown()

	w, _ := sys.Spawn(world.Props())
	s1, ch1 := collector(t, sys, "sess1")
	w.Send(world.Join{PlayerID: "p1", Name: "a", Session: s1})
	w.Send(world.Input{PlayerID: "p1", Left: true})

	// Merkezden sol kenara yeter de artar sayıda tick.
	needed := int(world.Width/2/(world.Speed/world.TickRate)) + 5
	for i := 0; i < needed; i++ {
		w.Send(world.Tick{})
	}
	var last world.Snapshot
	for i := 0; i < needed; i++ {
		last = nextSnap(t, ch1)
	}
	if last.Players[0].X != 0 {
		t.Fatalf("x = %v, want clamped at 0", last.Players[0].X)
	}
}
