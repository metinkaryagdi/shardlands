// Shardlands Faz 1 monolit prototip sunucusu.
//
//	go run ./cmd/server
//	# tarayıcı: http://localhost:8080
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"shardlands/services/server"
)

func main() {
	httpAddr := flag.String("http", ":8080", "gateway HTTP adresi")
	clientDir := flag.String("client", "client", "istemci dosyalarının dizini")
	dataDir := flag.String("data", "data", "event store dizini")
	flag.Parse()

	secret := []byte(os.Getenv("SHARDLANDS_SECRET"))
	if len(secret) == 0 {
		secret = []byte("dev-secret-change-me") // Faz 6: Vault
		log.Println("uyarı: SHARDLANDS_SECRET yok, geliştirme sırrı kullanılıyor")
	}

	// ARENA_NAMESPACE varsa arenalar CRD ile kümede açılır; yoksa nil
	// döner ve sunucu yerel sağlayıcıyı kullanır (tek süreç geliştirme).
	prov, err := k8sProvisioner()
	if err != nil {
		log.Fatal(err)
	}

	srv, err := server.Start(server.Config{
		HTTPAddr:        *httpAddr,
		PlayerAddr:      "127.0.0.1:9101",
		MatchmakingAddr: "127.0.0.1:9102",
		Secret:          secret,
		ClientDir:       *clientDir,
		DataDir:         *dataDir,
		Provisioner:     prov,
		// Boşsa player servisi bu süreçte açılır; Kubernetes'te ayrı
		// Pod'un Service adresi verilir (mesh politikası için gerçek
		// bir ağ atlaması gerekiyor — docs/service-mesh.md §5).
		PlayerTarget: os.Getenv("PLAYER_ADDR"),
		// Boşsa gömülü NATS; Kubernetes'te NATS_URL verilir.
		NATSURL: os.Getenv("NATS_URL"),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("shardlands hub: http://localhost%s", *httpAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("kapanıyor...")
	srv.Stop()
}
