package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"shardlands/pkg/trace"
)

// buffer'a JSON yazan bir logger kurar (New os.Stderr'e bağlı olduğu
// için testte handler'ı elle kuruyoruz — davranış aynı).
func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, nil)).With("service", "test")
}

func lastLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
		t.Fatalf("log JSON değil: %v (%q)", err, buf.String())
	}
	return m
}

// KORELASYONUN ASIL İDDİASI: bağlamda trace varsa log satırına
// trace_id düşer. Bu tutmazsa grafikten sebebe giden yol kopar.
func TestLogCarriesTraceID(t *testing.T) {
	rec := trace.NewRecorder("test", 4)
	ctx, span := rec.Start(context.Background(), "iş")
	defer span.End(nil)

	var buf bytes.Buffer
	FromContext(ctx, testLogger(&buf)).Info("bir şey oldu")

	m := lastLine(t, &buf)
	if m["trace_id"] != span.TraceID.String() {
		t.Fatalf("trace_id = %v, beklenen %s", m["trace_id"], span.TraceID)
	}
	if m["span_id"] != span.SpanID.String() {
		t.Fatalf("span_id = %v, beklenen %s", m["span_id"], span.SpanID)
	}
	if m["service"] != "test" {
		t.Fatalf("service alanı yok: %v", m["service"])
	}
}

// İzlenmeyen yol: trace bağlamı yoksa log YİNE YAZILIR, yalnız
// korelasyon alanı olmaz. İzleme kapalıyken loglamanın susması kabul
// edilemez.
func TestLogWithoutTraceStillWorks(t *testing.T) {
	var buf bytes.Buffer
	FromContext(context.Background(), testLogger(&buf)).Info("izsiz olay")

	m := lastLine(t, &buf)
	if _, ok := m["trace_id"]; ok {
		t.Fatal("trace yokken trace_id eklendi")
	}
	if m["msg"] != "izsiz olay" {
		t.Fatal("izsiz yolda mesaj kayboldu")
	}
}

// Sıfır trace kimliği "iz var" sayılmamalı (WithSpanContext ile boş
// bağlam sızarsa diye).
func TestZeroTraceIsNotCorrelated(t *testing.T) {
	ctx := trace.WithSpanContext(context.Background(), trace.SpanContext{})
	var buf bytes.Buffer
	FromContext(ctx, testLogger(&buf)).Info("sıfır iz")
	if _, ok := lastLine(t, &buf)["trace_id"]; ok {
		t.Fatal("sıfır trace_id korelasyona girdi")
	}
}
