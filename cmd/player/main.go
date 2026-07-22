// Player servisi: kimlik ve token basımı.
//
//	go run ./cmd/player -grpc :9101
//
// Faz 6'ya kadar bu servis hub süreciyle AYNI süreçte, loopback üstünden
// gRPC ile koşuyordu. Ayrılmasının sebebi ölçek değil GÜVENLİK: mesh
// politikası yalnız Pod sınırını geçen trafiği görebilir, loopback'i
// göremez. "Yalnız hub kimliği token basabilir" kuralının denetlenecek
// bir atlaması olsun diye servis gerçekten ayrıldı.
// Bkz. docs/service-mesh.md §5 ve docs/strangler-fig.md.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"

	"google.golang.org/grpc"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/services/player"
)

func main() {
	addr := flag.String("grpc", ":9101", "gRPC dinleme adresi")
	flag.Parse()

	secret := []byte(os.Getenv("SHARDLANDS_SECRET"))
	if len(secret) == 0 {
		secret = []byte("dev-secret-change-me") // Faz 6: Vault
		log.Println("uyarı: SHARDLANDS_SECRET yok, geliştirme sırrı kullanılıyor")
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("player: listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterPlayerServiceServer(gs, player.New(secret))
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("player: serve: %v", err)
		}
	}()
	log.Printf("player servisi %s üzerinde", lis.Addr())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("kapanıyor...")
	gs.GracefulStop()
}
