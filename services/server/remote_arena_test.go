package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/services/arena"
	"shardlands/services/matchmaking"
)

// remoteProvisioner, arenayı AYRI bir gRPC sunucusunda açar — Kubernetes
// kurulumundaki "arena Pod'u" ile aynı yapı (K8sProvisioner de yalnız
// Endpoint döner). Gateway'in uzak yolu bu sayede küme olmadan sınanır.
type remoteProvisioner struct {
	t *testing.T

	mu      sync.Mutex
	arenas  map[string]*arena.Arena
	servers map[string]*grpc.Server
}

func newRemoteProvisioner(t *testing.T) *remoteProvisioner {
	p := &remoteProvisioner{
		t: t, arenas: map[string]*arena.Arena{}, servers: map[string]*grpc.Server{},
	}
	t.Cleanup(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, gs := range p.servers {
			gs.Stop()
		}
	})
	return p
}

func (p *remoteProvisioner) Provision(_ context.Context, spec matchmaking.ArenaSpec) (*matchmaking.Handle, error) {
	a := arena.New(spec.ID, spec.Mode, spec.Players, arena.Options{OnEnd: spec.OnEnd})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	gs := grpc.NewServer()
	pb.RegisterArenaServiceServer(gs, arena.NewServer(a))
	go gs.Serve(lis)
	a.Run()

	p.mu.Lock()
	p.arenas[spec.ID] = a
	p.servers[spec.ID] = gs
	p.mu.Unlock()

	// DİKKAT: Arena alanı boş — gateway bunu uzak arena olarak görmeli.
	return &matchmaking.Handle{ID: spec.ID, Mode: string(spec.Mode), Endpoint: lis.Addr().String()}, nil
}

func (p *remoteProvisioner) Destroy(_ context.Context, arenaID string) error {
	p.mu.Lock()
	a, gs := p.arenas[arenaID], p.servers[arenaID]
	delete(p.arenas, arenaID)
	delete(p.servers, arenaID)
	p.mu.Unlock()
	if a != nil {
		a.Stop()
	}
	if gs != nil {
		gs.Stop()
	}
	return nil
}

func (p *remoteProvisioner) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.arenas)
}

// Faz 6'nın ilk dikey dilimi: arena AYRI bir süreçte (Pod) çalışırken
// oyuncu yine hub'dan arenaya geçer, komutları gateway vekil eder,
// kareler gRPC ile döner ve maç bitince oyuncu hub'a döner.
func TestRemoteArenaEndToEnd(t *testing.T) {
	prov := newRemoteProvisioner(t)
	srv, err := Start(Config{
		HTTPAddr: "127.0.0.1:0", PlayerAddr: "127.0.0.1:0", MatchmakingAddr: "127.0.0.1:0",
		Secret: []byte("e2e-secret"), ClientDir: tmpDir(t), DataDir: tmpDir(t),
		Provisioner: prov,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	_, tok1 := login(t, srv.HTTPAddr, "uzak1")
	_, tok2 := login(t, srv.HTTPAddr, "uzak2")
	ws1 := dialWS(t, srv.HTTPAddr, tok1)
	ws2 := dialWS(t, srv.HTTPAddr, tok2)
	ch1, ch2 := readerFor(ws1), readerFor(ws2)
	waitType(t, ch1, "welcome", 5*time.Second)
	waitType(t, ch2, "welcome", 5*time.Second)

	ws1.WriteJSON(map[string]any{"type": "queue", "mode": "1v1"})
	waitType(t, ch1, "queued", 5*time.Second)
	ws2.WriteJSON(map[string]any{"type": "queue", "mode": "1v1"})

	// UZAK arenaya alınmalılar.
	a1 := waitType(t, ch1, "arena-enter", 15*time.Second)
	a2 := waitType(t, ch2, "arena-enter", 15*time.Second)
	if a1.ArenaID == "" || a1.ArenaID != a2.ArenaID {
		t.Fatalf("arena ids differ: %q vs %q", a1.ArenaID, a2.ArenaID)
	}
	if a1.Mode != "1v1" {
		t.Fatalf("mode = %q, want 1v1 (uzak handle'dan taşınmalı)", a1.Mode)
	}
	if prov.count() != 1 {
		t.Fatalf("remote arenas = %d, want 1", prov.count())
	}

	// Kareler uzak süreçten akmalı.
	frame := waitType(t, ch1, "arena", 15*time.Second)
	if len(frame.Players) != 2 {
		t.Fatalf("remote frame has %d players, want 2", len(frame.Players))
	}

	// Komut vekil edilmeli: iki oyuncu da sağa gitsin, izlediğimiz
	// oyuncunun X'i artmalı.
	targetID := frame.Players[0].ID
	startX := playerXFromFrame(t, frame, targetID)
	ws1.WriteJSON(map[string]any{"type": "input", "right": true})
	ws2.WriteJSON(map[string]any{"type": "input", "right": true})

	moved := false
	deadline := time.Now().Add(15 * time.Second)
	for !moved && time.Now().Before(deadline) {
		f := waitType(t, ch1, "arena", 10*time.Second)
		if playerXFromFrame(t, f, targetID) > startX+5 {
			moved = true
		}
	}
	if !moved {
		t.Fatal("remote arena never reflected proxied input")
	}

	// Maçı bitir: bir oyuncunun bağlantısı kopsun.
	ws2.Close()
	hub := waitType(t, ch1, "hub-enter", 25*time.Second)
	if hub.Region == "" {
		t.Fatal("hub-enter without region after remote match")
	}
}

func playerXFromFrame(t *testing.T, m arenaMsg, id string) float64 {
	t.Helper()
	for _, p := range m.Players {
		if p.ID == id {
			return p.X
		}
	}
	t.Fatalf("player %s not in frame", id)
	return 0
}
