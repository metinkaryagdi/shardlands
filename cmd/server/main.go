// Shardlands Faz 1 monolit prototip sunucusu.
//
//	go run ./cmd/server
//	# tarayıcı: http://localhost:8080
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"shardlands/internal/keys"
	"shardlands/pkg/logging"
	"shardlands/services/server"
)

func main() {
	httpAddr := flag.String("http", ":8080", "gateway HTTP adresi")
	clientDir := flag.String("client", "client", "istemci dosyalarının dizini")
	dataDir := flag.String("data", "data", "event store dizini")
	flag.Parse()

	// Global default'ı EN BAŞTA JSON logger'a çevir: bundan sonraki her
	// satır (keys.Load'ın "anahtar kaynağı" logu dahil) yapılandırılmış
	// çıkar. Gateway kendi logger'ını ayrıca alıyor (korelasyon için);
	// bu, onun DIŞINDAKİ log'lar için (bu dosya, k8s istemcisi).
	slog.SetDefault(logging.New("hub"))

	// Anahtar zinciri: Vault varsa oradan, yoksa ortam değişkeninden.
	// Vault kullanılıyorsa zincir arka planda tazelenir ve anahtar
	// rotasyonu süreç yeniden başlatılmadan devreye girer.
	ctx, cancelKeys := context.WithCancel(context.Background())
	keyring, stopKeys, err := keys.Load(ctx)
	if err != nil {
		slog.Error("anahtarlar yüklenemedi", "err", err)
		os.Exit(1)
	}
	defer func() { stopKeys(); cancelKeys() }()

	// ARENA_NAMESPACE varsa arenalar CRD ile kümede açılır; yoksa nil
	// döner ve sunucu yerel sağlayıcıyı kullanır (tek süreç geliştirme).
	prov, err := k8sProvisioner()
	if err != nil {
		slog.Error("provisioner", "err", err)
		os.Exit(1)
	}

	srv, err := server.Start(server.Config{
		HTTPAddr:        *httpAddr,
		PlayerAddr:      "127.0.0.1:9101",
		MatchmakingAddr: "127.0.0.1:9102",
		Keys:            keyring,
		ClientDir:       *clientDir,
		DataDir:         *dataDir,
		Provisioner:     prov,
		// Boşsa player servisi bu süreçte açılır; Kubernetes'te ayrı
		// Pod'un Service adresi verilir (mesh politikası için gerçek
		// bir ağ atlaması gerekiyor — docs/service-mesh.md §5).
		PlayerTarget: os.Getenv("PLAYER_ADDR"),
		// Boşsa gömülü NATS; Kubernetes'te NATS_URL verilir.
		NATSURL: os.Getenv("NATS_URL"),
		// Maç kimliklerinin süreç ön eki.
		//
		// Neden Pod adı DEĞİL? StatefulSet Pod adı ("shardlands-0")
		// yeniden başlatmada aynı kalır; oysa çözmemiz gereken çakışma
		// tam olarak yeniden başlatmadan doğuyor — süreç içi sayaç
		// sıfırlanıp "m1"i tekrar üretiyor ve kümede duran eski
		// "arena-m1" kaydına çarpıyor (chaos deneyi 5).
		//
		// Bu yüzden ön ek SÜREÇ BAŞINA rastgele. Aynı anda birden çok
		// hub kopyası çalışsa onları da ayırırdı.
		Instance: processNonce(),
	})
	if err != nil {
		slog.Error("sunucu başlatılamadı", "err", err)
		os.Exit(1)
	}
	slog.Info("shardlands hub başladı", "http", *httpAddr)

	sig := make(chan os.Signal, 1)
	// SIGTERM: Kubernetes kapanışı bununla ister. Yalnız SIGINT
	// dinleyen bir süreç zarifçe kapanmaz, grace period dolunca
	// SIGKILL yer — Drain() hiç çalışmaz, oturumlar sertçe kopar.
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	slog.Info("kapanıyor")
	srv.Stop()
}

// processNonce, bu sürece özgü kısa bir ön ek üretir. Kaynak
// crypto/rand: zaman damgası kullansaydık hızlı yeniden başlatmalarda
// (aynı saniye içinde) çakışabilirdi.
func processNonce() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Rastgelelik alınamıyorsa çakışma riskini gizlemek yerine
		// açıkça bildir; süreç yine de çalışabilir.
		slog.Warn("süreç ön eki üretilemedi, kimlikler çakışabilir", "err", err)
		return ""
	}
	return hex.EncodeToString(b[:])
}
