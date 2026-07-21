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
	flag.Parse()

	secret := []byte(os.Getenv("SHARDLANDS_SECRET"))
	if len(secret) == 0 {
		secret = []byte("dev-secret-change-me") // Faz 6: Vault
		log.Println("uyarı: SHARDLANDS_SECRET yok, geliştirme sırrı kullanılıyor")
	}

	srv, err := server.Start(server.Config{
		HTTPAddr:        *httpAddr,
		PlayerAddr:      "127.0.0.1:9101",
		MatchmakingAddr: "127.0.0.1:9102",
		Secret:          secret,
		ClientDir:       *clientDir,
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
