// Package trace, W3C Trace Context yayılımını ve span kaydını SIFIRDAN
// uygular.
//
// # Neden metrikler yetmiyor?
//
// Faz 7'nin metrik adımında şunu ölçtük: gRPC istemci tarafı p95 7750µs,
// sunucu tarafı 475µs. Aradaki ~7.3ms'nin AĞ + MESH olduğunu
// çıkarsadık — ama bu bir çıkarımdı, ölçüm değil. Metrikler toplu
// (aggregate) çalışır; "şu tekil istek nerede bekledi" sorusunu
// cevaplayamazlar, çünkü tekil istekler ortalamanın içinde kaybolur.
//
// İzleme (tracing) tam bu boşluğu doldurur: bir isteğin servisler
// arasındaki yolculuğunu NEDENSELLİK AĞACI olarak kaydeder.
//
//	metrik → "ne kadar, ne oranda bozuk?"   (toplu, ucuz, sürekli)
//	log    → "bu olayda ne oldu?"            (tekil, ayrıntılı)
//	trace  → "bu istek nerede ne kadar durdu?" (tekil, yapısal)
//
// # Model
//
// Bir TRACE, tek bir mantıksal isteğin tamamıdır ve 16 baytlık bir
// kimliği vardır. İçindeki her iş birimi bir SPAN'dır (8 baytlık
// kimlik) ve kendisini doğuran span'ı parent olarak taşır. Sonuç bir
// ağaçtır:
//
//	POST /api/login                     [7.2ms]
//	  └─ PlayerService/CreatePlayer     [7.0ms]   ← ağ + mesh burada
//	       └─ (player süreci) handler   [0.4ms]
//
// # Neden elle yazıldı?
//
// OpenTelemetry SDK bu işin standardıdır ve üretimde tercih edilir.
// Ama projenin kuralı gereği önce mekanizmayı yazıyoruz — ve burada
// öğrenilecek şey bir SDK API'si değil, İKİ ZOR PROBLEM:
//
//  1. BAĞLAM YAYILIMI. Trace kimliği süreç içinde context.Context ile,
//     süreçler arasında ise BAŞLIKLARLA taşınır. Zincirin tek bir
//     halkasında düşerse trace ikiye bölünür ve "nerede durdu"
//     sorusu cevapsız kalır. Yayılımı elle yazmak, nerelerde
//     düşebileceğini görmenin en hızlı yolu.
//  2. ÖRNEKLEME (sampling). Her isteği kaydetmek imkânsızdır: 30Hz'lik
//     bir arena maçı dakikada 1800 kare üretir. Neyin kaydedileceği
//     kararı, kararın NEREDE verildiği kadar önemlidir (aşağıya bak).
//
// # Örnekleme kararı bir kez verilir ve zincir boyunca TAŞINIR
//
// traceparent'ın `flags` baytındaki "sampled" biti, kararı ilk veren
// servisin (bizde gateway) kararını aşağıya taşır. Her servis kendi
// başına karar verseydi trace'ler yarım kalırdı: gateway kaydeder,
// player kaydetmez, ağaç kopuk görünür.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// TraceID ve SpanID, W3C'nin belirlediği genişlikler.
type (
	TraceID [16]byte
	SpanID  [8]byte
)

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

func (t TraceID) IsZero() bool { return t == TraceID{} }

// SpanContext, bir span'ı tanımlayan ve SÜREÇ SINIRINI GEÇEN bilgi.
// Span'ın adı, süresi ve nitelikleri geçmez — yalnız kimlik ve
// örnekleme kararı geçer. Taşınan verinin küçük tutulması bilinçli:
// her RPC'ye eklenen her bayt, her çağrıda ödenir.
type SpanContext struct {
	TraceID TraceID
	SpanID  SpanID
	Sampled bool
}

type ctxKey struct{}

// FromContext, bağlamdaki span bağlamını verir.
func FromContext(ctx context.Context) (SpanContext, bool) {
	sc, ok := ctx.Value(ctxKey{}).(SpanContext)
	return sc, ok
}

// WithSpanContext, bağlama span bağlamı yerleştirir.
func WithSpanContext(ctx context.Context, sc SpanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, sc)
}

// ---- W3C traceparent ----
//
// Biçim:  00-<32 hex trace-id>-<16 hex span-id>-<2 hex flags>
// Örnek:  00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//
// İlk alan SÜRÜMDÜR ve bilerek ayrı: ileride alan eklenirse eski
// uygulamalar sürümü görüp anlamadıklarını atlayabilsin diye.

const TraceparentHeader = "traceparent"

// Traceparent, span bağlamını başlık değerine çevirir.
func (sc SpanContext) Traceparent() string {
	flags := "00"
	if sc.Sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", sc.TraceID, sc.SpanID, flags)
}

// ParseTraceparent, başlığı çözer. Hatalı başlık HATA DEĞİL sessiz
// başarısızlıktır (ok=false): bozuk bir başlık yüzünden isteği
// reddetmek, gözlem katmanının işleve zarar vermesi olurdu — Faz 6'daki
// "gözlem, gözlediği sistemi düşürmemeli" ilkesinin aynısı.
func ParseTraceparent(v string) (SpanContext, bool) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || parts[0] != "00" {
		return SpanContext{}, false
	}
	tid, err := hex.DecodeString(parts[1])
	if err != nil || len(tid) != 16 {
		return SpanContext{}, false
	}
	sid, err := hex.DecodeString(parts[2])
	if err != nil || len(sid) != 8 {
		return SpanContext{}, false
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil || len(flags) != 1 {
		return SpanContext{}, false
	}
	var sc SpanContext
	copy(sc.TraceID[:], tid)
	copy(sc.SpanID[:], sid)
	sc.Sampled = flags[0]&0x01 == 1
	// Sıfır kimlikler geçersizdir (spec): "hiç yok"la karıştırılmasın.
	if sc.TraceID.IsZero() || sc.SpanID == (SpanID{}) {
		return SpanContext{}, false
	}
	return sc, true
}

// ---- Span ----

// Span, tek bir iş birimi.
type Span struct {
	TraceID  TraceID
	SpanID   SpanID
	ParentID SpanID
	Name     string
	Service  string
	Start    time.Time
	Duration time.Duration
	Err      string
	Sampled  bool

	rec  *Recorder
	once sync.Once
}

// End, span'ı kapatır ve (örneklenmişse) kaydeder.
func (s *Span) End(err error) {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.Duration = time.Since(s.Start)
		if err != nil {
			s.Err = err.Error()
		}
		if s.Sampled && s.rec != nil {
			s.rec.add(s)
		}
	})
}

// Context, bu span'ın bağlamıdır (alt span'lar bunu parent alır).
func (s *Span) Context() SpanContext {
	if s == nil {
		return SpanContext{}
	}
	return SpanContext{TraceID: s.TraceID, SpanID: s.SpanID, Sampled: s.Sampled}
}

// Recorder, biten span'ları tutar.
//
// Üretimde buranın yerine OTLP dışa aktarıcı (Jaeger/Tempo) gelir.
// Halka tampon (ring buffer) seçilmesinin sebebi gözlem katmanının
// SINIRSIZ BELLEK TÜKETMEMESİ: izleme, izlediği süreci OOM'a
// sokmamalı.
type Recorder struct {
	Service string

	mu    sync.Mutex
	buf   []*Span
	next  int
	full  bool
	limit int
}

func NewRecorder(service string, limit int) *Recorder {
	if limit <= 0 {
		limit = 512
	}
	return &Recorder{Service: service, buf: make([]*Span, limit), limit: limit}
}

func (r *Recorder) add(s *Span) {
	r.mu.Lock()
	r.buf[r.next] = s
	r.next = (r.next + 1) % r.limit
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
}

// Spans, kayıtlı span'ların kopyasını (eskiden yeniye) verir.
func (r *Recorder) Spans() []*Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*Span
	if r.full {
		out = append(out, r.buf[r.next:]...)
	}
	out = append(out, r.buf[:r.next]...)
	return out
}

// Start, yeni bir span açar.
//
// Bağlamda span varsa ONUN ÇOCUĞU olur (aynı trace, yeni span id);
// yoksa YENİ BİR TRACE başlar ve örnekleme kararı burada verilir.
// Karar bir kez verilir ve traceparent ile aşağı taşınır.
func (r *Recorder) Start(ctx context.Context, name string) (context.Context, *Span) {
	s := &Span{Name: name, Service: r.Service, Start: time.Now(), rec: r}
	if parent, ok := FromContext(ctx); ok && !parent.TraceID.IsZero() {
		s.TraceID = parent.TraceID
		s.ParentID = parent.SpanID
		s.Sampled = parent.Sampled
	} else {
		rand.Read(s.TraceID[:])
		s.Sampled = r.sample()
	}
	rand.Read(s.SpanID[:])
	return WithSpanContext(ctx, s.Context()), s
}

// SampleRate, kök span'ların örneklenme oranı (0..1). Varsayılan 1:
// bu projede giriş/takas yolları düşük hacimli, hepsini kaydetmek
// ucuz. Arena kare yolu gibi yüksek hacimli yollar ZATEN hiç span
// açmıyor — örnekleme oranıyla değil, ENSTRÜMANTASYON KARARIYLA
// dışarıda tutuluyorlar. Doğru sıralama budur: önce neyi hiç izlemeyeceğine
// karar ver, sonra kalanı örnekle.
var SampleRate = 1.0

func (r *Recorder) sample() bool {
	if SampleRate >= 1 {
		return true
	}
	if SampleRate <= 0 {
		return false
	}
	var b [1]byte
	rand.Read(b[:])
	return float64(b[0])/255.0 < SampleRate
}
