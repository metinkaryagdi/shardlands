package arena_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/services/arena"
)

// startArenaServer, gerçek bir arena gRPC sunucusu ayağa kaldırır
// (arena Pod'unun test karşılığı) ve adresini döner.
func startArenaServer(t *testing.T, a *arena.Arena) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterArenaServiceServer(gs, arena.NewServer(a))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func dialPlay(t *testing.T, addr, playerID string) (pb.ArenaService_PlayClient, func()) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := pb.NewArenaServiceClient(conn).Play(ctx)
	if err != nil {
		cancel()
		conn.Close()
		t.Fatal(err)
	}
	if err := stream.Send(&pb.PlayRequest{
		Msg: &pb.PlayRequest_Join{Join: &pb.JoinArena{PlayerId: playerID}},
	}); err != nil {
		cancel()
		conn.Close()
		t.Fatal(err)
	}
	return stream, func() { cancel(); conn.Close() }
}

func duelArena(onEnd func(arena.Result)) *arena.Arena {
	return arena.New("arena-remote", arena.Mode1v1, []arena.PlayerSpec{
		{ID: "p1", Name: "bir", Team: 0},
		{ID: "p2", Name: "iki", Team: 1},
	}, arena.Options{OnEnd: onEnd})
}

// Uzak oynanış: komut gRPC ile gider, kareler gRPC ile döner ve hareket
// gerçekten uygulanır.
func TestRemotePlayMovesPlayer(t *testing.T) {
	a := duelArena(nil)
	addr := startArenaServer(t, a)
	a.Run()
	t.Cleanup(a.Stop)

	stream, closeFn := dialPlay(t, addr, "p1")
	defer closeFn()

	// İlk kareyi bekle: başlangıç konumunu al.
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first frame: %v", err)
	}
	startX := playerX(t, arena.FromProtoSnapshot(first.GetSnapshot()), "p1")

	// Sağa hareket komutu gönder.
	if err := stream.Send(&pb.PlayRequest{Msg: &pb.PlayRequest_Command{
		Command: arena.ToProtoCommand(arena.Command{Kind: arena.CmdMove, Right: true}),
	}}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if x := playerX(t, arena.FromProtoSnapshot(resp.GetSnapshot()), "p1"); x > startX+5 {
			return // hareket uzaktan uygulandı
		}
	}
	t.Fatal("player never moved via remote commands")
}

func playerX(t *testing.T, s arena.Snapshot, id string) float64 {
	t.Helper()
	for _, p := range s.Players {
		if p.ID == id {
			return p.X
		}
	}
	t.Fatalf("player %s not in snapshot", id)
	return 0
}

// Arenaya ait olmayan oyuncu reddedilir (yetkisiz akış açılamaz).
func TestRemotePlayRejectsUnknownPlayer(t *testing.T) {
	a := duelArena(nil)
	addr := startArenaServer(t, a)

	stream, closeFn := dialPlay(t, addr, "yabancı")
	defer closeFn()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("unknown player was accepted")
	}
}

// İlk mesaj Join değilse akış reddedilir.
func TestRemotePlayRequiresJoinFirst(t *testing.T) {
	a := duelArena(nil)
	addr := startArenaServer(t, a)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := pb.NewArenaServiceClient(conn).Play(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&pb.PlayRequest{Msg: &pb.PlayRequest_Command{
		Command: arena.ToProtoCommand(arena.Command{Kind: arena.CmdMove}),
	}})
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream without Join was accepted")
	}
}

// Akış kapanınca oyuncu ayrılmış sayılır ve maç sonlanır.
func TestRemoteDisconnectEndsMatch(t *testing.T) {
	done := make(chan arena.Result, 1)
	a := duelArena(func(r arena.Result) { done <- r })
	addr := startArenaServer(t, a)
	a.Run()
	t.Cleanup(a.Stop)

	stream, closeFn := dialPlay(t, addr, "p2")
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	closeFn() // bağlantıyı kopar

	select {
	case r := <-done:
		if r.WinnerTeam != 0 {
			t.Fatalf("winner = %d, want 0 (p2 disconnected)", r.WinnerTeam)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("match did not end after remote disconnect")
	}
}
