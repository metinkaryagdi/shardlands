package actor_test

import (
	"sync/atomic"
	"testing"
	"time"

	"shardlands/pkg/actor"
)

// crasher: string mesajda panic'ler, int mesajda toplama yapıp koşan
// toplamı sums kanalına bildirir. Producer her restart'ta yenisini
// yaratacağı için sum alanı restart'ta doğal olarak sıfırlanır.
type crasher struct {
	starts *atomic.Int32
	sums   chan int
	sum    int
}

func (c *crasher) PreStart(*actor.Context) { c.starts.Add(1) }
func (c *crasher) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case string:
		panic(m)
	case int:
		c.sum += m
		c.sums <- c.sum
	}
}

func crasherProps(name string, starts *atomic.Int32, sums chan int, strat actor.Strategy) actor.Props {
	return actor.Props{
		Name:        name,
		Producer:    func() actor.Actor { return &crasher{starts: starts, sums: sums} },
		Supervision: strat,
	}
}

// Panic'te varsayılan davranış Restart: state sıfırlanır ama mailbox
// korunur — panic'ten sonra kuyruğa girmiş mesajlar yeni instance
// tarafından işlenir.
func TestRestartResetsStateKeepsMailbox(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	var starts atomic.Int32
	sums := make(chan int)
	ref := mustSpawn(t, s, crasherProps("c", &starts, sums, actor.Strategy{}))

	ref.Send(1)
	ref.Send(2)
	ref.Send("boom") // restart tetikler
	ref.Send(4)      // restart'tan sonra işlenmeli

	if got := <-sums; got != 1 {
		t.Fatalf("first sum = %d, want 1", got)
	}
	if got := <-sums; got != 3 {
		t.Fatalf("second sum = %d, want 3", got)
	}
	// 4 yeni instance'a gitti: toplam 4 olmalı (7 olsaydı state sızmış demekti).
	if got := <-sums; got != 4 {
		t.Fatalf("post-restart sum = %d, want 4 (state must reset)", got)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("PreStart ran %d times, want 2", got)
	}
}

// Pencere içinde restart sınırı aşılırsa aktör durdurulmalı; sonrası
// dead letter olmalı.
func TestRestartLimitStopsActor(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	var starts atomic.Int32
	sums := make(chan int)
	strat := actor.Strategy{MaxRestarts: 2, Window: time.Minute}
	ref := mustSpawn(t, s, crasherProps("c", &starts, sums, strat))

	ref.Send("a") // restart 1
	ref.Send("b") // restart 2
	ref.Send("c") // limit aşıldı -> stop
	waitStopped(t, ref)

	if got := starts.Load(); got != 3 {
		t.Fatalf("PreStart ran %d times, want 3 (initial + 2 restarts)", got)
	}

	before := s.DeadLetterCount()
	ref.Send(1)
	if got := s.DeadLetterCount(); got != before+1 {
		t.Fatalf("dead letters = %d, want %d", got, before+1)
	}
}

// Resume: hata yutulur, instance ve state korunur.
func TestResumeKeepsState(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	var starts atomic.Int32
	sums := make(chan int)
	strat := actor.Strategy{
		Decider: func(error) actor.Directive { return actor.DirectiveResume },
	}
	ref := mustSpawn(t, s, crasherProps("c", &starts, sums, strat))

	ref.Send(1)
	ref.Send("boom") // yutulur
	ref.Send(2)

	if got := <-sums; got != 1 {
		t.Fatalf("first sum = %d, want 1", got)
	}
	if got := <-sums; got != 3 {
		t.Fatalf("post-resume sum = %d, want 3 (state must survive)", got)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("PreStart ran %d times, want 1 (no restart)", got)
	}
}

// Decider Stop derse aktör tek panic'te kalıcı durmalı.
func TestStopDirective(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	var starts atomic.Int32
	sums := make(chan int)
	strat := actor.Strategy{
		Decider: func(error) actor.Directive { return actor.DirectiveStop },
	}
	ref := mustSpawn(t, s, crasherProps("c", &starts, sums, strat))

	ref.Send("boom")
	waitStopped(t, ref)
	if got := starts.Load(); got != 1 {
		t.Fatalf("PreStart ran %d times, want 1", got)
	}
}

// Escalate: çocuk durur, hata ebeveyne yükselir; ebeveynin stratejisi
// (varsayılan Restart) ebeveyni yeniden başlatır ve PreStart'ı çocuğu
// yeniden yaratır.
func TestEscalateRestartsParent(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	var parentStarts, childStarts atomic.Int32
	childRefs := make(chan *actor.Ref, 2)
	sums := make(chan int)

	escalating := actor.Strategy{
		Decider: func(error) actor.Directive { return actor.DirectiveEscalate },
	}
	mustSpawn(t, s, actor.Props{
		Name: "parent",
		Producer: func() actor.Actor {
			return &spawningParent{
				starts:     &parentStarts,
				childRefs:  childRefs,
				childProps: crasherProps("child", &childStarts, sums, escalating),
			}
		},
	})

	child1 := <-childRefs
	child1.Send("boom") // escalate -> parent restart -> yeni çocuk

	waitStopped(t, child1)
	var child2 *actor.Ref
	select {
	case child2 = <-childRefs: // restart olan ebeveynin yarattığı yeni çocuk
	case <-time.After(2 * time.Second):
		t.Fatal("parent did not respawn child after escalation")
	}

	// Yeni çocuğun PreStart'ı kendi goroutine'inde asenkron koşar; bir
	// mesaj işletip cevabını bekleyerek başladığından emin oluyoruz.
	child2.Send(1)
	select {
	case got := <-sums:
		if got != 1 {
			t.Fatalf("new child sum = %d, want 1 (fresh state)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new child did not process messages")
	}

	if got := parentStarts.Load(); got != 2 {
		t.Fatalf("parent PreStart ran %d times, want 2", got)
	}
	if got := childStarts.Load(); got != 2 {
		t.Fatalf("child PreStart ran %d times, want 2", got)
	}
}

// spawningParent: PreStart'ta çocuk yaratıp Ref'ini teste bildirir.
type spawningParent struct {
	starts     *atomic.Int32
	childRefs  chan *actor.Ref
	childProps actor.Props
}

func (p *spawningParent) Receive(*actor.Context) {}
func (p *spawningParent) PreStart(ctx *actor.Context) {
	p.starts.Add(1)
	ref, err := ctx.Spawn(p.childProps)
	if err != nil {
		panic(err)
	}
	p.childRefs <- ref
}
