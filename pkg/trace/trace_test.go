package trace

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// W3C biçimi bir SÖZLEŞMEDİR: başka dillerde yazılmış servisler ve
// hazır araçlar (Jaeger, Tempo) bu biçime göre okur. Round-trip testi,
// kendi yazdığımız çözümleyicinin standarttan sapmadığını garanti eder.
func TestTraceparentRoundTrip(t *testing.T) {
	rec := NewRecorder("test", 8)
	_, span := rec.Start(context.Background(), "kok")
	sc := span.Context()

	got, ok := ParseTraceparent(sc.Traceparent())
	if !ok {
		t.Fatalf("kendi ürettiğimiz başlık çözümlenemedi: %q", sc.Traceparent())
	}
	if got.TraceID != sc.TraceID || got.SpanID != sc.SpanID || got.Sampled != sc.Sampled {
		t.Fatalf("round-trip bozuk: %+v != %+v", got, sc)
	}
	// Spec'in tam uzunluğu: 2+1+32+1+16+1+2
	if len(sc.Traceparent()) != 55 {
		t.Fatalf("traceparent uzunluğu = %d, beklenen 55", len(sc.Traceparent()))
	}
}

// Bozuk başlık HATA DEĞİL sessiz başarısızlık olmalı: gözlem katmanı,
// gözlediği sistemi düşürmemeli.
func TestParseRejectsGarbage(t *testing.T) {
	kotu := []string{
		"", "bozuk",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // sürüm
		"00-kisa-00f067aa0ba902b7-01",                             // trace id
		"00-4bf92f3577b34da6a3ce929d0e0e4736-kisa-01",             // span id
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01", // sıfır trace
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // sıfır span
	}
	for _, v := range kotu {
		if _, ok := ParseTraceparent(v); ok {
			t.Errorf("geçersiz başlık kabul edildi: %q", v)
		}
	}
}

// Zincirin ASIL İDDİASI: alt span aynı trace'te kalır ve parent'ı
// doğru gösterir. Bu tutmazsa "nerede durdu" sorusu cevapsız kalır.
func TestChildInheritsTrace(t *testing.T) {
	rec := NewRecorder("test", 8)
	ctx, kok := rec.Start(context.Background(), "POST /api/login")
	_, cocuk := rec.Start(ctx, "client CreatePlayer")

	if cocuk.TraceID != kok.TraceID {
		t.Fatal("çocuk farklı trace'e düştü — zincir koptu")
	}
	if cocuk.ParentID != kok.SpanID {
		t.Fatal("çocuğun parent'ı kök değil")
	}
	if cocuk.SpanID == kok.SpanID {
		t.Fatal("çocuk kökle aynı span id'yi aldı")
	}
}

// Süreç sınırını geçiş: traceparent'tan gelen bağlam, sanki yerel bir
// parent'mış gibi devam etmeli.
func TestResumeAcrossProcessBoundary(t *testing.T) {
	istemci := NewRecorder("gateway", 8)
	sunucu := NewRecorder("player", 8)

	_, cs := istemci.Start(context.Background(), "client CreatePlayer")
	baslik := cs.Context().Traceparent()

	// ... ağ ...
	sc, ok := ParseTraceparent(baslik)
	if !ok {
		t.Fatal("başlık çözümlenemedi")
	}
	_, ss := sunucu.Start(WithSpanContext(context.Background(), sc), "server CreatePlayer")

	if ss.TraceID != cs.TraceID {
		t.Fatal("trace süreç sınırında koptu")
	}
	if ss.ParentID != cs.SpanID {
		t.Fatal("sunucu span'ının parent'ı istemci span'ı değil")
	}
	if ss.Service != "player" || cs.Service != "gateway" {
		t.Fatal("servis adları karıştı")
	}
}

// Örnekleme kararı BİR KEZ verilir ve aşağı taşınır. Her servis kendi
// başına karar verseydi ağaç kopuk görünürdü.
func TestSamplingDecisionPropagates(t *testing.T) {
	rec := NewRecorder("test", 8)
	sc := SpanContext{TraceID: TraceID{1}, SpanID: SpanID{2}, Sampled: false}
	_, span := rec.Start(WithSpanContext(context.Background(), sc), "alt")
	if span.Sampled {
		t.Fatal("üst örneklenmemişken alt örneklendi")
	}
	span.End(nil)
	if len(rec.Spans()) != 0 {
		t.Fatal("örneklenmemiş span kaydedildi")
	}
}

func TestRecorderRingBufferAndError(t *testing.T) {
	rec := NewRecorder("test", 2)
	for i := 0; i < 3; i++ {
		_, s := rec.Start(context.Background(), "s")
		s.End(errors.New("patladı"))
	}
	got := rec.Spans()
	if len(got) != 2 {
		t.Fatalf("halka tampon %d span tuttu, beklenen 2", len(got))
	}
	if !strings.Contains(got[0].Err, "patladı") {
		t.Fatalf("hata kaydedilmedi: %q", got[0].Err)
	}
}

// End iki kez çağrılırsa süre ve kayıt bozulmamalı (defer + elle
// çağrının aynı yolda buluşması yaygın bir kaza).
func TestDoubleEndIsSafe(t *testing.T) {
	rec := NewRecorder("test", 4)
	_, s := rec.Start(context.Background(), "s")
	s.End(nil)
	ilk := s.Duration
	s.End(errors.New("ikinci"))
	if s.Duration != ilk || s.Err != "" {
		t.Fatal("ikinci End span'ı bozdu")
	}
	if len(rec.Spans()) != 1 {
		t.Fatal("span iki kez kaydedildi")
	}
}
