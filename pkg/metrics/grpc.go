package metrics

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// gRPC için RED metrikleri (Rate, Errors, Duration).
//
// # Neden interceptor?
//
// Her handler'a elle sayaç koymak üç şeyi garanti eder: unutulan
// handler'lar, tutarsız metrik adları ve kopyala-yapıştır hataları.
// Interceptor bir KESİŞEN İLGİDİR (cross-cutting concern) ve tam da
// bunun için var: tek yerde yazılır, bütün metotlara uygulanır.
//
// Aynı fikri Faz 6'da mesh ile görmüştük — orada şifreleme ve kimlik
// uygulama kodundan çıkıp sidecar'a taşınmıştı. Burada ölçüm, handler
// kodundan çıkıp interceptor'a taşınıyor. Fark şu: mesh SÜREÇ DIŞINDA
// olduğu için uygulamanın iç anlamını bilemez (hangi maç, hangi saga
// adımı); interceptor süreç içinde olduğu için gRPC durum kodunu
// doğrudan okuyabilir.
//
// # İki taraf, iki farklı hikâye
//
// Sunucu tarafı "ben ne kadar sürede cevapladım" der; istemci tarafı
// "ben ne kadar bekledim" der. İkisinin farkı AĞ + KUYRUK + mesh
// proxy'sidir. Yalnız sunucu tarafını ölçen bir sistem, ağdan gelen
// gecikmeyi hiç göremez ve "bizde her şey yolunda" der.

var (
	// GRPCServerHandled, sunucu tarafında işlenen çağrılar.
	//
	// Etiketler kardinalite açısından güvenli: metot adları kodda
	// SABİTTİR (proto sözleşmesi), durum kodları sonlu bir kümedir.
	GRPCServerHandled = NewCounterVecOnce("shardlands_grpc_server_handled_total",
		"Sunucu tarafında işlenen gRPC çağrıları.", "method", "code")

	GRPCServerDuration = NewHistogramOnce("shardlands_grpc_server_duration_seconds",
		"gRPC çağrısının sunucu tarafındaki süresi.",
		[]float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1})

	// GRPCClientHandled, istemci tarafı — ağı ve mesh'i de kapsar.
	GRPCClientHandled = NewCounterVecOnce("shardlands_grpc_client_handled_total",
		"İstemci tarafından yapılan gRPC çağrıları.", "method", "code")

	GRPCClientDuration = NewHistogramOnce("shardlands_grpc_client_duration_seconds",
		"gRPC çağrısının istemci tarafındaki süresi (ağ ve mesh dahil).",
		[]float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1, 2.5})
)

// UnaryServerInterceptor, sunucu tarafı RED ölçümü.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		basla := time.Now()
		resp, err := handler(ctx, req)
		m := shortMethod(info.FullMethod)
		GRPCServerDuration.Observe(time.Since(basla).Seconds())
		GRPCServerHandled.WithLabelValues(m, status.Code(err).String()).Inc()
		return resp, err
	}
}

// UnaryClientInterceptor, istemci tarafı RED ölçümü.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		basla := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		m := shortMethod(method)
		GRPCClientDuration.Observe(time.Since(basla).Seconds())
		GRPCClientHandled.WithLabelValues(m, status.Code(err).String()).Inc()
		return err
	}
}

// shortMethod, "/shardlands.v1.PlayerService/CreatePlayer" biçimini
// "PlayerService/CreatePlayer"a indirir. Tam yol etiket olarak
// gereksiz uzun; paket adı zaten sabit.
func shortMethod(full string) string {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}
