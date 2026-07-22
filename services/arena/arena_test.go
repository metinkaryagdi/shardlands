package arena

import (
	"testing"
	"time"
)

func duel(t *testing.T, onEnd func(Result)) *Arena {
	t.Helper()
	return New("a1", Mode1v1, []PlayerSpec{
		{ID: "p1", Name: "bir", Team: 0},
		{ID: "p2", Name: "iki", Team: 1},
	}, Options{OnEnd: onEnd})
}

func find(s Snapshot, id string) PlayerState {
	for _, p := range s.Players {
		if p.ID == id {
			return p
		}
	}
	return PlayerState{}
}

// Hareket: komut kuyruğa yazılır, tick onu toplu boşaltıp uygular.
func TestMovementFromQueuedInput(t *testing.T) {
	a := duel(t, nil)
	start := find(a.Snapshot(), "p1")

	a.Push(Command{PlayerID: "p1", Kind: CmdMove, Right: true})
	a.Tick()

	got := find(a.Snapshot(), "p1")
	want := start.X + Speed*dt
	if got.X != want {
		t.Fatalf("x = %v, want %v", got.X, want)
	}
	// Girdi DURUMU kalıcı: yeni komut gelmeden ikinci tick de ilerletir.
	a.Tick()
	if got2 := find(a.Snapshot(), "p1"); got2.X != want+Speed*dt {
		t.Fatalf("second tick x = %v, want %v", got2.X, want+Speed*dt)
	}
}

// Ateş: mermi uçar, düşmana isabet edince hasar verir ve yok olur.
func TestFireProjectileHitsEnemy(t *testing.T) {
	a := duel(t, nil)
	a.Push(Command{PlayerID: "p1", Kind: CmdFire, AimX: 1, AimY: 0})
	a.Tick()

	if n := len(a.Snapshot().Projectiles); n != 1 {
		t.Fatalf("projectiles = %d, want 1", n)
	}

	// Mesafeyi kapatacak kadar tick.
	for i := 0; i < 60; i++ {
		a.Tick()
		if find(a.Snapshot(), "p2").Health < MaxHealth {
			break
		}
	}
	p2 := find(a.Snapshot(), "p2")
	if p2.Health != MaxHealth-ProjectileDamage {
		t.Fatalf("p2 health = %d, want %d", p2.Health, MaxHealth-ProjectileDamage)
	}
	if n := len(a.Snapshot().Projectiles); n != 0 {
		t.Fatalf("projectile survived the hit: %d", n)
	}
}

// Ateş bekleme süresi: art arda komutlar tek mermi üretir.
func TestFireCooldown(t *testing.T) {
	a := duel(t, nil)
	for i := 0; i < 5; i++ {
		a.Push(Command{PlayerID: "p1", Kind: CmdFire, AimX: 0, AimY: 1})
	}
	a.Tick()
	if n := len(a.Snapshot().Projectiles); n != 1 {
		t.Fatalf("projectiles = %d, want 1 (cooldown)", n)
	}
}

// Takım elenince maç biter, kazanan bildirilir ve OnEnd bir kez çağrılır.
func TestEliminationEndsMatch(t *testing.T) {
	var results []Result
	a := duel(t, func(r Result) { results = append(results, r) })

	a.byID["p2"].health = ProjectileDamage // tek isabetle düşecek
	a.Push(Command{PlayerID: "p1", Kind: CmdFire, AimX: 1, AimY: 0})
	for i := 0; i < 80 && !a.Snapshot().Over; i++ {
		a.Tick()
	}

	snap := a.Snapshot()
	if !snap.Over || snap.WinnerTeam != 0 {
		t.Fatalf("snapshot over=%v winner=%d, want true/0", snap.Over, snap.WinnerTeam)
	}
	res, ok := a.Result()
	if !ok || res.WinnerTeam != 0 {
		t.Fatalf("result = %+v ok=%v", res, ok)
	}
	if len(results) != 1 {
		t.Fatalf("OnEnd called %d times, want 1", len(results))
	}
	if res.Damage["p1"] != ProjectileDamage {
		t.Fatalf("damage p1 = %d, want %d", res.Damage["p1"], ProjectileDamage)
	}
	if len(res.Survivors) != 1 || res.Survivors[0] != "p1" {
		t.Fatalf("survivors = %v, want [p1]", res.Survivors)
	}
	// Bitmiş maçta ek tick bir şey değiştirmemeli (idempotent son).
	a.Tick()
	if r2, _ := a.Result(); r2.Ticks != res.Ticks {
		t.Fatal("ticking after end changed the result")
	}
}

// Süre dolunca canı fazla olan takım kazanır.
func TestTimeLimitDecidesByHealth(t *testing.T) {
	a := duel(t, nil)
	a.byID["p2"].health = MaxHealth - 30 // p1 önde

	for i := 0; i < MatchTicks+2 && !a.Snapshot().Over; i++ {
		a.Tick()
	}
	snap := a.Snapshot()
	if !snap.Over {
		t.Fatal("match did not end at time limit")
	}
	if snap.WinnerTeam != 0 {
		t.Fatalf("winner = %d, want 0 (more health)", snap.WinnerTeam)
	}
	if snap.RemainingMs != 0 {
		t.Fatalf("remaining = %d, want 0", snap.RemainingMs)
	}
}

// Eşit canla süre dolarsa beraberlik.
func TestTimeLimitDraw(t *testing.T) {
	a := duel(t, nil)
	for i := 0; i < MatchTicks+2 && !a.Snapshot().Over; i++ {
		a.Tick()
	}
	if w := a.Snapshot().WinnerTeam; w != -1 {
		t.Fatalf("winner = %d, want -1 (draw)", w)
	}
}

// Ayrılan oyuncu elenir ve maç biter.
func TestLeaveEliminatesPlayer(t *testing.T) {
	a := duel(t, nil)
	a.Push(Command{PlayerID: "p2", Kind: CmdLeave})
	a.Tick()
	snap := a.Snapshot()
	if !snap.Over || snap.WinnerTeam != 0 {
		t.Fatalf("over=%v winner=%d, want true/0", snap.Over, snap.WinnerTeam)
	}
}

// 2v2: dört oyuncu, iki takım; takım arkadaşına ateş hasar VERMEZ.
func TestTeamFireDoesNotDamageAlly(t *testing.T) {
	a := New("a2", Mode2v2, []PlayerSpec{
		{ID: "a1", Team: 0}, {ID: "a2", Team: 0},
		{ID: "b1", Team: 1}, {ID: "b2", Team: 1},
	}, Options{})

	// a1 (80,200) → a2 (80,290) yönünde ateş: aynı takım.
	a.Push(Command{PlayerID: "a1", Kind: CmdFire, AimX: 0, AimY: 1})
	for i := 0; i < 30; i++ {
		a.Tick()
	}
	if h := find(a.Snapshot(), "a2").Health; h != MaxHealth {
		t.Fatalf("ally health = %d, want %d (no friendly fire)", h, MaxHealth)
	}
}

// Kuyruk taşarsa komut düşer (gecikme öncelikli profil: eskimiş girdiyi
// beklemek yerine at).
func TestInputQueueOverflowDrops(t *testing.T) {
	a := duel(t, nil)
	pushed := 0
	for i := 0; i < inputCapacity*2; i++ {
		if a.Push(Command{PlayerID: "p1", Kind: CmdMove, Right: true}) {
			pushed++
		}
	}
	if pushed > inputCapacity {
		t.Fatalf("accepted %d commands, capacity %d", pushed, inputCapacity)
	}
	if a.Dropped() == 0 {
		t.Fatal("no drops recorded on overflow")
	}
}

// Gerçek zamanlı döngü: Run/Stop çalışır ve maç bitince kendi durur.
func TestRunLoopStopsWhenMatchEnds(t *testing.T) {
	done := make(chan Result, 1)
	a := duel(t, func(r Result) { done <- r })
	a.byID["p2"].health = ProjectileDamage
	a.Run()
	a.Push(Command{PlayerID: "p1", Kind: CmdFire, AimX: 1, AimY: 0})

	select {
	case r := <-done:
		if r.WinnerTeam != 0 {
			t.Fatalf("winner = %d", r.WinnerTeam)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("match never ended under Run loop")
	}
	a.Wait()
}
