package gateway

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/actor"
	"shardlands/services/arena"
)

// arenaLink, oturumun arenaya komut iletme yoludur. İki gerçekleşme:
//
//	localLink  → arena BU süreçte; komut doğrudan lock-free kuyruğa
//	remoteLink → arena bir Pod'da; komut gRPC akışıyla vekil edilir
//
// Oturum ikisini ayırt etmez. Uzak yolda kareler gRPC akışından okunup
// oturum aktörüne aynı arena.Snapshot mesajı olarak gönderilir; böylece
// session'ın alma tarafı hiç değişmedi.
type arenaLink interface {
	ID() string
	Mode() string
	Push(c arena.Command)
	Close()
}

// ---- yerel ----

type localLink struct{ a *arena.Arena }

func (l localLink) ID() string           { return l.a.ID() }
func (l localLink) Mode() string         { return string(l.a.Mode()) }
func (l localLink) Push(c arena.Command) { l.a.Push(c) }
func (l localLink) Close()               {}

// ---- uzak (arena Pod'u) ----

type remoteLink struct {
	id     string
	mode   string
	conn   *grpc.ClientConn
	stream pb.ArenaService_PlayClient
	cancel context.CancelFunc

	mu     sync.Mutex // gRPC akışında TEK yazar olmalı
	closed bool
}

func (r *remoteLink) ID() string   { return r.id }
func (r *remoteLink) Mode() string { return r.mode }

func (r *remoteLink) Push(c arena.Command) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	// Send hatası bağlantının bittiğini gösterir; oturum zaten maç sonu
	// veya kopuşla temizlenecek.
	_ = r.stream.Send(&pb.PlayRequest{
		Msg: &pb.PlayRequest_Command{Command: arena.ToProtoCommand(c)},
	})
}

func (r *remoteLink) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()

	r.stream.CloseSend()
	r.cancel()
	r.conn.Close()
}

// dialRemoteArena, arena Pod'una bağlanır, oyuncuyu kaydeder ve gelen
// kareleri oturum aktörüne akıtan okuyucuyu başlatır.
//
// Not: kimlik/şifreleme burada YOK — bu hop'u service mesh (mTLS)
// koruyacak (Faz 6'nın sonraki adımı). Mesh olmayan kurulumda düz
// metindir; bu bilinçli ve README'de yazılı.
func dialRemoteArena(endpoint, arenaID, mode, playerID string, sess *actor.Ref) (*remoteLink, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("gateway: dial arena %s: %w", endpoint, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := pb.NewArenaServiceClient(conn).Play(ctx)
	if err != nil {
		cancel()
		conn.Close()
		return nil, fmt.Errorf("gateway: open arena stream: %w", err)
	}
	if err := stream.Send(&pb.PlayRequest{
		Msg: &pb.PlayRequest_Join{Join: &pb.JoinArena{PlayerId: playerID}},
	}); err != nil {
		cancel()
		conn.Close()
		return nil, fmt.Errorf("gateway: join arena: %w", err)
	}

	l := &remoteLink{id: arenaID, mode: mode, conn: conn, stream: stream, cancel: cancel}

	// Okuyucu: uzak kareleri oturuma yerel snapshot mesajı olarak taşı.
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				return // maç bitti veya bağlantı koptu
			}
			sess.Send(arena.FromProtoSnapshot(resp.GetSnapshot()))
		}
	}()
	return l, nil
}
