// Package logging, üç sinyalin ÜÇÜNCÜ ayağını (yapılandırılmış log)
// kurar ve onu diğer ikisine (metrik, izleme) BAĞLAR.
//
// # Neden yapılandırılmış log?
//
// `log.Printf("player %s created", id)` insan okuması için iyidir,
// makine okuması için değildir. Kümede log'lar bir toplayıcıya
// (Loki/Elasticsearch) akar ve orada SORGULANIR: "şu trace'in bütün
// satırları", "şu oyuncunun son hatası", "p99'u bozan istekler". Serbest
// metin bunu yapamaz; her satır bir olayın YAPILANDIRILMIŞ kaydı
// olmalıdır (anahtar=değer).
//
// slog (Go 1.21+) bunu standart kütüphaneyle sağlar — bu yüzden burada
// harici bir kütüphane YOK. Faz 0'ın "çekirdek kütüphanesiz" kuralının
// mümkün olduğu bir yer daha.
//
// # ASIL MESELE: korelasyon
//
// Üç sinyal ayrı ayrı işe yaramaz; birbirine bağlandıklarında işe yarar.
// Tipik hata ayıklama yolculuğu şudur:
//
//	metrik  → "p99 gecikme fırladı"          (bir GRAFİK gördün)
//	trace   → "şu trace 3sn player'da durdu" (bir İSTEK buldun)
//	log     → "o trace'te token imzası hata verdi" (SEBEBİ okudun)
//
// İkinci oktan üçüncüye geçişi sağlayan şey TEK BİR ALANDIR: trace_id.
// Log satırında trace_id yoksa, grafikten sebebe giden yol kopar ve
// "yavaşlık" saatlerce açıklanamaz kalır. Bu paketin bütün varlık
// sebebi o alanı her ilgili satıra otomatik koymaktır.
package logging

import (
	"context"
	"log/slog"
	"os"

	"shardlands/pkg/trace"
)

// New, JSON çıktılı bir logger kurar.
//
// Neden JSON, neden metin değil? Kümede log'u insan değil makine okur;
// JSON toplayıcıların ayrıştırmadan indeksleyebildiği biçimdir. Yerel
// geliştirmede metin daha okunası olurdu — o ayrımı ortam belirler
// (aşağıda), ama VARSAYILAN makine biçimidir çünkü üretim varsayılan
// olmalıdır, geliştirme istisna.
func New(service string) *slog.Logger {
	var h slog.Handler
	opts := &slog.HandlerOptions{Level: level()}
	if os.Getenv("LOG_FORMAT") == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	// service alanı HER satırda: bir toplayıcıda bütün servislerin
	// log'ları tek akışta buluşur, ayırmanın yolu bu alandır.
	return slog.New(h).With("service", service)
}

func level() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// FromContext, bağlamdaki trace_id/span_id'yi otomatik ekleyen bir
// logger döner. KORELASYONUN KALBİ burasıdır: çağıran elle trace id
// taşımaz, bağlam taşır.
//
// Trace bağlamı yoksa (izlenmeyen yol) sade logger döner — log yine
// yazılır, yalnız korelasyon alanı olmaz. İzleme kapalıyken loglamanın
// susması kabul edilemezdi.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	sc, ok := trace.FromContext(ctx)
	if !ok || sc.TraceID.IsZero() {
		return base
	}
	return base.With(
		"trace_id", sc.TraceID.String(),
		"span_id", sc.SpanID.String(),
	)
}
