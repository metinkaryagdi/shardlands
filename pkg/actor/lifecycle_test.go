package actor_test

import (
	"sync"
	"testing"
	"time"

	"shardlands/pkg/actor"
)

// Ebeveyn durdurulunca önce çocuğun, sonra ebeveynin PostStop'u çalışmalı
// (aşağıdan yukarıya temizlik).
func TestStopCascadeOrder(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	events := make(chan string, 4)
	parent := mustSpawn(t, s, actor.Props{
		Name: "parent",
		Producer: func() actor.Actor {
			return &cascadeParent{events: events}
		},
	})

	// Çocuğun spawn olduğundan emin ol.
	select {
	case e := <-events:
		if e != "child-started" {
			t.Fatalf("first event = %q, want child-started", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("child never started")
	}

	parent.Stop()
	waitStopped(t, parent)

	first, second := <-events, <-events
	if first != "child-stopped" || second != "parent-stopped" {
		t.Fatalf("stop order = [%s, %s], want [child-stopped, parent-stopped]", first, second)
	}
}

type cascadeParent struct{ events chan string }

func (p *cascadeParent) Receive(*actor.Context) {}
func (p *cascadeParent) PreStart(ctx *actor.Context) {
	_, err := ctx.Spawn(actor.Props{
		Name: "child",
		Producer: func() actor.Actor {
			return &hookActor{
				onPreStart: func() { p.events <- "child-started" },
				onPostStop: func() { p.events <- "child-stopped" },
			}
		},
	})
	if err != nil {
		panic(err)
	}
}
func (p *cascadeParent) PostStop(*actor.Context) { p.events <- "parent-stopped" }

// Stop kontrol kuyruğundan gider: işlenmekte olan mesaj bitince, mailbox'ta
// bekleyen mesajlar işlenmeden aktör durmalı ve bekleyenler dead letter olmalı.
func TestStopBeatsQueuedMessages(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	processed := 0

	ref := mustSpawn(t, s, actor.Props{Producer: func() actor.Actor {
		return actor.ReceiverFunc(func(ctx *actor.Context) {
			processed++
			select {
			case started <- struct{}{}:
			default:
			}
			<-gate
		})
	}})

	for i := 0; i < 5; i++ {
		ref.Send(i)
	}
	<-started // ilk mesaj işleniyor, kalan 4'ü mailbox'ta
	ref.Stop()
	close(gate)
	waitStopped(t, ref)

	if processed != 1 {
		t.Fatalf("processed %d messages, want 1 (stop must preempt queue)", processed)
	}
	if got := s.DeadLetterCount(); got != 4 {
		t.Fatalf("dead letters = %d, want 4 (drained mailbox)", got)
	}

	ref.Send("late")
	if got := s.DeadLetterCount(); got != 5 {
		t.Fatalf("dead letters = %d, want 5 (send after stop)", got)
	}
}

// DropNewest: mailbox doluyken gelen mesaj bloklamak yerine düşmeli.
func TestDropNewestOverflow(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	gate := make(chan struct{})
	// Test bir assert'te düşerse bile aktörü gate'te asılı bırakma;
	// yoksa defer'deki Shutdown, bloklu Receive'i sonsuza dek bekler.
	openGate := sync.OnceFunc(func() { close(gate) })
	defer openGate()
	processed := make(chan string, 8)

	ref := mustSpawn(t, s, actor.Props{
		MailboxSize: 2, // ring buffer asgari kapasitesi (cap-1 protokolde çakışıyor)
		Overflow:    actor.DropNewest,
		Producer: func() actor.Actor {
			return actor.ReceiverFunc(func(ctx *actor.Context) {
				if m, ok := ctx.Message().(string); ok {
					processed <- m
					if m == "m1" {
						<-gate // ilk mesajda blokla ki buffer kontrollü dolsun
					}
				}
			})
		},
	})

	ref.Send("m1")
	if m := <-processed; m != "m1" {
		t.Fatalf("first processed = %q, want m1", m)
	}
	// m1 işleniyor (gate'te bloklu), buffer boş.
	ref.Send("m2") // buffer'a girer
	ref.Send("m3") // buffer'a girer (cap 2)
	ref.Send("m4") // buffer dolu -> düşer
	if got := s.DeadLetterCount(); got != 1 {
		t.Fatalf("dead letters = %d, want 1 (m4 dropped)", got)
	}

	openGate()
	if m := <-processed; m != "m2" {
		t.Fatalf("second processed = %q, want m2", m)
	}
	if m := <-processed; m != "m3" {
		t.Fatalf("third processed = %q, want m3", m)
	}
	// m2 ve m3 işlendi, buffer boş: Poison artık düşmeden sıraya girebilir.
	ref.Poison()
	waitStopped(t, ref)

	select {
	case m := <-processed:
		t.Fatalf("unexpected extra message processed: %q", m)
	default:
	}
}
