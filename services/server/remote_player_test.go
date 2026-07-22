package server

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/services/player"
)

// TestRemotePlayerService, player servisi AYRI BİR SÜREÇTE (burada:
// ayrı bir gRPC dinleyicisi) koştuğunda giriş yolunun bozulmadığını
// doğrular.
//
// Bu ayrımın sebebi ölçek değil güvenlikti: mesh proxy'si loopback
// trafiğini yakalayamadığı için, gateway→player atlamasının Pod sınırını
// geçmesi gerekiyordu (docs/service-mesh.md §5). Test, "kod aynı kalsın,
// topoloji değişsin" iddiasının bedava olmadığını gösteriyor — kanıtı
// burada duruyor.
func TestRemotePlayerService(t *testing.T) {
	secret := []byte("e2e-secret")

	// Hub'dan bağımsız bir player servisi.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterPlayerServiceServer(gs, player.New(secret))
	go gs.Serve(lis)
	t.Cleanup(gs.GracefulStop)

	srv, err := Start(Config{
		HTTPAddr: "127.0.0.1:0",
		// PlayerAddr BİLEREK boş: PlayerTarget verildiğinde hub'ın
		// kendi player sunucusunu açmaması gerekiyor.
		PlayerTarget:    lis.Addr().String(),
		MatchmakingAddr: "127.0.0.1:0",
		Secret:          secret,
		ClientDir:       tmpDir(t),
		DataDir:         tmpDir(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	id, token := login(t, srv.HTTPAddr, "uzak")
	if id == "" || token == "" {
		t.Fatalf("uzak player servisiyle giriş başarısız: id=%q token=%q", id, token)
	}

	// Token'ı basan uzak servis, doğrulayan hub: ikisi aynı sırrı
	// paylaşıyor. Oturum açılabiliyorsa zincir tamam.
	conn := dialWS(t, srv.HTTPAddr, token)
	var got wireMsg
	if err := conn.ReadJSON(&got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "welcome" || got.ID != id {
		t.Fatalf("ilk mesaj = %+v, beklenen welcome/%s", got, id)
	}
}
