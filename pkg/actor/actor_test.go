package actor_test

import (
	"sync/atomic"
	"testing"
	"time"

	"shardlands/pkg/actor"
)

func mustSpawn(t *testing.T, s *actor.System, p actor.Props) *actor.Ref {
	t.Helper()
	ref, err := s.Spawn(p)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	return ref
}

func waitStopped(t *testing.T, ref *actor.Ref) {
	t.Helper()
	select {
	case <-ref.Stopped():
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not stop in time", ref.Path())
	}
}

// Bir aktör mesajları gönderim sırasıyla (FIFO) ve tek tek işlemeli.
func TestFIFOOrdering(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	var got []int
	ref := mustSpawn(t, s, actor.Props{Producer: func() actor.Actor {
		return actor.ReceiverFunc(func(ctx *actor.Context) {
			if n, ok := ctx.Message().(int); ok {
				got = append(got, n)
			}
		})
	}})

	const n = 200 // mailbox (64) taşsın ki Block backpressure de çalışsın
	for i := 0; i < n; i++ {
		ref.Send(i)
	}
	ref.Poison()
	waitStopped(t, ref)

	if len(got) != n {
		t.Fatalf("processed %d messages, want %d", len(got), n)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("got[%d] = %d, want %d (out of order)", i, v, i)
		}
	}
}

// ctx.Send gönderen bilgisini taşımalı; Sender'a cevapla ping-pong kur.
func TestPingPongWithSender(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	const rounds = 20
	done := make(chan int, 1)

	pong := mustSpawn(t, s, actor.Props{Producer: func() actor.Actor {
		return actor.ReceiverFunc(func(ctx *actor.Context) {
			if n, ok := ctx.Message().(int); ok {
				ctx.Send(ctx.Sender(), n+1)
			}
		})
	}})

	ping := mustSpawn(t, s, actor.Props{Producer: func() actor.Actor {
		return actor.ReceiverFunc(func(ctx *actor.Context) {
			switch m := ctx.Message().(type) {
			case string: // "start"
				ctx.Send(pong, 0)
			case int:
				if m >= rounds {
					done <- m
					return
				}
				ctx.Send(pong, m+1)
			}
		})
	}})

	ping.Send("start")
	select {
	case n := <-done:
		if n < rounds {
			t.Fatalf("finished at %d, want >= %d", n, rounds)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ping-pong did not finish")
	}
}

// Çok sayıda goroutine aynı aktöre gönderirken mesaj kaybolmamalı;
// aktör içi state lock'suz güvenli olmalı (-race ile anlamlı).
func TestConcurrentSenders(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	const senders, perSender = 10, 100
	total := senders * perSender
	count := 0
	done := make(chan struct{})

	ref := mustSpawn(t, s, actor.Props{Producer: func() actor.Actor {
		return actor.ReceiverFunc(func(ctx *actor.Context) {
			count++
			if count == total {
				close(done)
			}
		})
	}})

	for i := 0; i < senders; i++ {
		go func() {
			for j := 0; j < perSender; j++ {
				ref.Send(j)
			}
		}()
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("processed %d/%d messages", count, total)
	}
}

// Aynı ebeveyn altında isim çakışması hata döndürmeli.
func TestDuplicateName(t *testing.T) {
	s := actor.NewSystem("test")
	defer s.Shutdown()

	props := actor.Props{Name: "dup", Producer: func() actor.Actor {
		return actor.ReceiverFunc(func(*actor.Context) {})
	}}
	mustSpawn(t, s, props)
	if _, err := s.Spawn(props); err == nil {
		t.Fatal("expected duplicate name error, got nil")
	}
}

// Shutdown üst seviye aktörleri durdurmalı ve PostStop çağrılmalı.
func TestSystemShutdown(t *testing.T) {
	s := actor.NewSystem("test")
	var stopped atomic.Int32
	mustSpawn(t, s, actor.Props{Producer: func() actor.Actor {
		return &hookActor{onPostStop: func() { stopped.Add(1) }}
	}})
	s.Shutdown()
	if got := stopped.Load(); got != 1 {
		t.Fatalf("PostStop ran %d times, want 1", got)
	}
}

// hookActor: lifecycle kancalarını test etmek için yardımcı aktör.
type hookActor struct {
	onPreStart func()
	onPostStop func()
	onMessage  func(ctx *actor.Context)
}

func (h *hookActor) Receive(ctx *actor.Context) {
	if h.onMessage != nil {
		h.onMessage(ctx)
	}
}
func (h *hookActor) PreStart(*actor.Context) {
	if h.onPreStart != nil {
		h.onPreStart()
	}
}
func (h *hookActor) PostStop(*actor.Context) {
	if h.onPostStop != nil {
		h.onPostStop()
	}
}
