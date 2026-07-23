package trace

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Bağlam yayılımı: süreç içinde context.Context, süreçler arasında
// BAŞLIK. Zincirin tek bir halkası düşerse trace ikiye bölünür.
//
// Bu dosyadaki üç fonksiyon, bu projedeki üç sınırın tamamını kapsar:
//
//	HTTP  : tarayıcı → gateway            (HTTPMiddleware)
//	gRPC  : gateway → player / arena      (Unary*Interceptor)
//
// Dördüncü bir sınır daha var ve BİLEREK kapsanmadı: WebSocket
// üstünden gelen oyuncu komutları. Sebep aşağıda.

// HTTPMiddleware, gelen isteğin traceparent'ını okur (yoksa yeni trace
// başlatır) ve bir span açar.
func (r *Recorder) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if sc, ok := ParseTraceparent(req.Header.Get(TraceparentHeader)); ok {
			ctx = WithSpanContext(ctx, sc)
		}
		ctx, span := r.Start(ctx, req.Method+" "+req.URL.Path)
		// Yanıt başlığına da koy: istemci, hangi trace'e düştüğünü
		// bilsin. Hata ayıklamada "şu isteğin trace id'si neydi"
		// sorusunu cevaplayan tek şey bu.
		w.Header().Set(TraceparentHeader, span.Context().Traceparent())
		next.ServeHTTP(w, req.WithContext(ctx))
		span.End(nil)
	})
}

// UnaryClientInterceptor, giden gRPC çağrısına traceparent ekler.
func (r *Recorder) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := r.Start(ctx, "client "+shortMethod(method))
		ctx = metadata.AppendToOutgoingContext(ctx,
			TraceparentHeader, span.Context().Traceparent())
		err := invoker(ctx, method, req, reply, cc, opts...)
		span.End(err)
		return err
	}
}

// UnaryServerInterceptor, gelen gRPC çağrısındaki traceparent'ı okur.
func (r *Recorder) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(TraceparentHeader); len(vals) > 0 {
				if sc, ok := ParseTraceparent(vals[0]); ok {
					ctx = WithSpanContext(ctx, sc)
				}
			}
		}
		ctx, span := r.Start(ctx, "server "+shortMethod(info.FullMethod))
		resp, err := handler(ctx, req)
		span.End(err)
		return resp, err
	}
}

func shortMethod(full string) string {
	if i := len(full) - 1; i >= 0 {
		for j := len(full) - 1; j >= 0; j-- {
			if full[j] == '.' {
				return full[j+1:]
			}
		}
	}
	return full
}

// ---- WebSocket neden kapsanmadı ----
//
// Oyuncu komutları (input, gather, chat) WS üstünden geliyor ve her
// biri için span açmak teknik olarak mümkün. Yapılmadı, çünkü:
//
//  1. HACİM. 20Hz girdi akışında oyuncu başına saniyede onlarca komut
//     var; span'lar faydalı sinyali gürültüye boğardı.
//  2. YAPI UYUMSUZLUĞU. Trace bir İSTEK-CEVAP ağacı varsayar; WS
//     oturumu ise uzun ömürlü, çift yönlü bir AKIŞ. Doğal karşılığı
//     "oturum = bir trace" olurdu ve o trace saatlerce açık kalırdı —
//     hiçbir izleme arka ucu bunu iyi göstermez.
//
// Doğru araç ayrımı: WS akışının sağlığı METRİKLERLE izleniyor
// (dead letters = doygunluk, ws_sessions = kullanım). İzleme, istek-
// cevap şeklindeki yollara ayrıldı: giriş, takas, maç kurulumu.
//
// "Her şeyi izle" bir hedef değil, bir maliyet hatasıdır.
