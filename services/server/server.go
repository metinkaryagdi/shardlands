// Package server, Faz 1 monolit prototipinin montajıdır: player ve
// matchmaking gRPC servisleri, world aktörü, tick zamanlayıcısı ve
// gateway TEK süreçte ama gerçek ağ sınırlarıyla (ayrı TCP portları,
// gerçek gRPC çağrıları) çalışır. Faz 4'teki strangler-fig anlatısının
// "önce"si bu paket: süreçleri ayırmak = bu dosyayı bölmek, kod değil
// topoloji değişir.
package server

import (
	"context"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/actor"
	"shardlands/services/gateway"
	"shardlands/services/matchmaking"
	"shardlands/services/player"
	"shardlands/services/world"
)

type Config struct {
	HTTPAddr        string // örn ":8080"; test için "127.0.0.1:0"
	PlayerAddr      string // player gRPC dinleme adresi
	MatchmakingAddr string
	Secret          []byte
	ClientDir       string
}

type Server struct {
	HTTPAddr string // gerçek adres (":0" çözülmüş hali)

	httpSrv  *http.Server
	grpcSrvs []*grpc.Server
	conns    []*grpc.ClientConn
	system   *actor.System
	stopTick chan struct{}
}

// Start, tüm bileşenleri ayağa kaldırır ve dinlemeye başlar.
func Start(cfg Config) (*Server, error) {
	s := &Server{stopTick: make(chan struct{})}

	// İç servisler: gerçek gRPC sunucuları (in-process ama ağ üstünde).
	playerAddr, err := s.serveGRPC(cfg.PlayerAddr, func(gs *grpc.Server) {
		pb.RegisterPlayerServiceServer(gs, player.New(cfg.Secret))
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.serveGRPC(cfg.MatchmakingAddr, func(gs *grpc.Server) {
		pb.RegisterMatchmakingServiceServer(gs, matchmaking.New())
	}); err != nil {
		s.Stop()
		return nil, err
	}

	// Gateway'in servis istemcileri.
	playerConn, err := grpc.NewClient(playerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.Stop()
		return nil, err
	}
	s.conns = append(s.conns, playerConn)

	// Dünya: aktör + dış tick zamanlayıcısı.
	s.system = actor.NewSystem("shardlands")
	worldRef, err := s.system.Spawn(world.Props())
	if err != nil {
		s.Stop()
		return nil, err
	}
	go func() {
		t := time.NewTicker(time.Second / world.TickRate)
		defer t.Stop()
		for {
			select {
			case <-s.stopTick:
				return
			case <-t.C:
				worldRef.Send(world.Tick{})
			}
		}
	}()

	// Gateway (HTTP + WS).
	httpLis, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		s.Stop()
		return nil, err
	}
	s.HTTPAddr = httpLis.Addr().String()
	s.httpSrv = &http.Server{Handler: gateway.New(gateway.Config{
		Secret:    cfg.Secret,
		ClientDir: cfg.ClientDir,
		System:    s.system,
		World:     worldRef,
		Players:   pb.NewPlayerServiceClient(playerConn),
	})}
	go s.httpSrv.Serve(httpLis)

	return s, nil
}

func (s *Server) serveGRPC(addr string, register func(*grpc.Server)) (string, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	gs := grpc.NewServer()
	register(gs)
	s.grpcSrvs = append(s.grpcSrvs, gs)
	go gs.Serve(lis)
	return lis.Addr().String(), nil
}

// Stop, bileşenleri ters sırayla kapatır: önce dış kapı (HTTP), sonra
// dünya/aktörler, en son iç servisler.
func (s *Server) Stop() {
	if s.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s.httpSrv.Shutdown(ctx)
		cancel()
	}
	close(s.stopTick)
	if s.system != nil {
		s.system.Shutdown()
	}
	for _, c := range s.conns {
		c.Close()
	}
	for _, gs := range s.grpcSrvs {
		gs.GracefulStop()
	}
}
