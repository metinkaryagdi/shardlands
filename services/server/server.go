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
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/services/chat"
	"shardlands/services/gateway"
	"shardlands/services/inventory"
	"shardlands/services/matchmaking"
	"shardlands/services/player"
	"shardlands/services/stats"
	"shardlands/services/trade"
	"shardlands/services/world"
)

type Config struct {
	HTTPAddr        string // örn ":8080"; test için "127.0.0.1:0"
	PlayerAddr      string // player gRPC dinleme adresi
	MatchmakingAddr string
	Secret          []byte
	ClientDir       string
	DataDir         string   // event store'un yaşadığı dizin
	Shards          []string // shard node id'leri (boşsa iki node varsayılan)
}

type Server struct {
	HTTPAddr string // gerçek adres (":0" çözülmüş hali)

	httpSrv  *http.Server
	grpcSrvs []*grpc.Server
	conns    []*grpc.ClientConn
	system   *actor.System
	events   *es.Store
	chatHist *chat.History
	inv      *inventory.Inventory
	stats    *stats.Stats
	router   *world.Router
	stopTick chan struct{}
	stopOnce sync.Once
}

// Router, CAP deneyi ve gözlem için shard router'ına erişim verir.
func (s *Server) Router() *world.Router { return s.router }

// Start, tüm bileşenleri ayağa kaldırır ve dinlemeye başlar.
func Start(cfg Config) (*Server, error) {
	if len(cfg.Shards) == 0 {
		cfg.Shards = []string{"shard-0", "shard-1"}
	}
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

	// Event store + read model'ler.
	events, err := es.Open(filepath.Join(cfg.DataDir, "events"))
	if err != nil {
		s.Stop()
		return nil, err
	}
	s.events = events
	s.chatHist = chat.NewHistory(events)
	s.inv = inventory.New(events)
	s.stats = stats.New(events, "world-0") // Faz 3: shard başına ayrı nodeID
	trades := trade.NewOrchestrator(events)

	// Dünya: bölgelere ayrılmış, consistent hashing ile shard'lara atanmış
	// bölge aktörleri + dış tick zamanlayıcısı (tüm bölgelere).
	s.system = actor.NewSystem("shardlands")
	router, err := world.NewHub(s.system, events, cfg.Shards)
	if err != nil {
		s.Stop()
		return nil, err
	}
	s.router = router
	go func() {
		t := time.NewTicker(time.Second / world.TickRate)
		defer t.Stop()
		for {
			select {
			case <-s.stopTick:
				return
			case <-t.C:
				for _, ref := range router.Refs() {
					ref.Send(world.Tick{})
				}
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
		Router:    router,
		Players:   pb.NewPlayerServiceClient(playerConn),
		Chat:      s.chatHist,
		Inventory: s.inv,
		Trades:    trades,
		Stats:     s.stats,
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
// dünya/aktörler (event üreticileri), sonra projection ve event store,
// en son iç servisler. İdempotent: kapanış birden çok yoldan
// tetiklenebilir (sinyal + defer + test cleanup).
func (s *Server) Stop() { s.stopOnce.Do(s.stop) }

func (s *Server) stop() {
	if s.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s.httpSrv.Shutdown(ctx)
		cancel()
	}
	close(s.stopTick)
	if s.system != nil {
		s.system.Shutdown()
	}
	if s.chatHist != nil {
		s.chatHist.Close()
	}
	if s.inv != nil {
		s.inv.Close()
	}
	if s.stats != nil {
		s.stats.Close()
	}
	if s.events != nil {
		s.events.Close()
	}
	for _, c := range s.conns {
		c.Close()
	}
	for _, gs := range s.grpcSrvs {
		gs.GracefulStop()
	}
}
