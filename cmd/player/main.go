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
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/internal/keys"
	"shardlands/services/player"
)

func main() {
	addr := flag.String("grpc", ":9101", "gRPC dinleme adresi")
	flag.Parse()

	// Anahtar zinciri: Vault varsa oradan. Player TOKEN BASAN taraf
	// olduğu için rotasyonda kritik olan bu süreçtir — yeni anahtar
	// buraya ulaşmadan imzalama dönmez.
	ctx, cancelKeys := context.WithCancel(context.Background())
	keyring, stopKeys, err := keys.Load(ctx)
	if err != nil {
		log.Fatalf("player: anahtarlar yüklenemedi: %v", err)
	}
	defer func() { stopKeys(); cancelKeys() }()

	// Kopya ön eki: Pod adı (aşağı yönlü API) — bir namespace içinde
	// benzersizdir. Yoksa hostname'e düş; o da yoksa tek kopya say.
	instance := os.Getenv("POD_NAME")
	if instance == "" {
		instance, _ = os.Hostname()
	}
	instance = shortInstance(instance)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("player: listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterPlayerServiceServer(gs, player.NewKeyring(keyring, instance))
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("player: serve: %v", err)
		}
	}()
	log.Printf("player servisi %s üzerinde (kopya: %q)", lis.Addr(), instance)

	// SIGTERM de dinleniyor: Kubernetes kapanışı bununla ister,
	// SIGINT'le değil. Yalnız Interrupt dinleyen bir süreç kümede
	// zarifçe kapanmaz, terminationGracePeriod dolunca SIGKILL yer.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("kapanıyor...")
	gs.GracefulStop() // akıştaki çağrıları bitir, yenisini alma
}

// shortInstance, Pod adını kimliğe gömülecek kadar kısaltır.
// "player-6b6b9f7b67-kprj2" -> "kprj2"
func shortInstance(s string) string {
	if i := strings.LastIndex(s, "-"); i >= 0 && i+1 < len(s) {
		return s[i+1:]
	}
	return s
}
