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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	pb "shardlands/gen/shardlands/v1"

	"shardlands/internal/keys"
	"shardlands/pkg/logging"
	"shardlands/pkg/metrics"
	"shardlands/pkg/trace"
	"shardlands/services/player"
)

func main() {
	addr := flag.String("grpc", ":9101", "gRPC dinleme adresi")
	adminAddr := flag.String("admin", ":9102", "yönetim (metrik/sağlık) adresi")
	flag.Parse()

	// Logger'ı EN BAŞTA kur ve global default yap: bundan sonraki her
	// log satırı (keys.Load'ın "anahtar kaynağı" satırı dahil) JSON
	// biçimde ve service alanıyla çıkar. Sıra önemli — SetDefault'tan
	// önceki hiçbir log yapılandırılmış olmaz.
	logger := logging.New("player")
	slog.SetDefault(logger)

	// Anahtar zinciri: Vault varsa oradan. Player TOKEN BASAN taraf
	// olduğu için rotasyonda kritik olan bu süreçtir — yeni anahtar
	// buraya ulaşmadan imzalama dönmez.
	ctx, cancelKeys := context.WithCancel(context.Background())
	keyring, stopKeys, err := keys.Load(ctx)
	if err != nil {
		slog.Error("anahtarlar yüklenemedi", "err", err)
		os.Exit(1)
	}
	defer func() { stopKeys(); cancelKeys() }()

	// Kopya ön eki: Pod adı (aşağı yönlü API) — bir namespace içinde
	// benzersizdir. Yoksa hostname'e düş; o da yoksa tek kopya say.
	instance := os.Getenv("POD_NAME")
	if instance == "" {
		instance, _ = os.Hostname()
	}
	instance = shortInstance(instance)

	// YÖNETİM SUNUCUSU: /metrics ve /healthz.
	//
	// Player yalnız gRPC konuşuyordu; Prometheus ise HTTP çeker. Ayrı
	// bir port açmak, gRPC portunu çift protokole zorlamaktan basit ve
	// mesh politikası tarafında da temiz: yönetim portu yalnız
	// Prometheus kimliğine açılırken 9101 yalnız hub'a açık kalıyor.
	// Player kendi izleyicisini kurar ve gelen traceparent'ı okur:
	// span'ları hub'ın kaydına yazamaz (ayrı süreç), ama AYNI TRACE
	// kimliğini taşır. Gerçek kurulumda ikisi de aynı arka uca
	// (Jaeger/Tempo) gönderir ve ağaç orada birleşir.
	tracer := trace.NewRecorder("player", 256)

	adminMux := http.NewServeMux()
	adminMux.Handle("GET /metrics", metrics.Handler())
	adminMux.Handle("GET /debug/traces", tracesHandler(tracer))
	adminMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	adminSrv := &http.Server{Addr: *adminAddr, Handler: adminMux}
	go func() {
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("admin sunucusu", "err", err)
		}
	}()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("dinlenemedi", "addr", *addr, "err", err)
		os.Exit(1)
	}
	// Interceptor: RED metrikleri tek yerde, bütün metotlar için.
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		tracer.UnaryServerInterceptor()))
	pb.RegisterPlayerServiceServer(gs, player.NewKeyring(keyring, instance).WithLogger(logger))
	go func() {
		if err := gs.Serve(lis); err != nil {
			slog.Error("gRPC serve", "err", err)
		}
	}()
	slog.Info("player başladı", "addr", lis.Addr().String(), "instance", instance)

	// SIGTERM de dinleniyor: Kubernetes kapanışı bununla ister,
	// SIGINT'le değil. Yalnız Interrupt dinleyen bir süreç kümede
	// zarifçe kapanmaz, terminationGracePeriod dolunca SIGKILL yer.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	slog.Info("kapanıyor")
	gs.GracefulStop() // akıştaki çağrıları bitir, yenisini alma
	adminSrv.Close()
}

// shortInstance, Pod adını kimliğe gömülecek kadar kısaltır.
// "player-6b6b9f7b67-kprj2" -> "kprj2"
func shortInstance(s string) string {
	if i := strings.LastIndex(s, "-"); i >= 0 && i+1 < len(s) {
		return s[i+1:]
	}
	return s
}

// tracesHandler, player'ın kendi span'larını gösterir. Hub'daki
// karşılığıyla aynı trace kimliğini taşırlar; zincirin süreç sınırını
// geçtiği buradan doğrulanır.
func tracesHandler(rec *trace.Recorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		for _, s := range rec.Spans() {
			fmt.Fprintf(w, "trace=%s span=%s parent=%s %-28s %7.3fms %s\n",
				s.TraceID, s.SpanID.String()[:8], s.ParentID.String()[:8],
				s.Name, float64(s.Duration.Microseconds())/1000, s.Err)
		}
	})
}
