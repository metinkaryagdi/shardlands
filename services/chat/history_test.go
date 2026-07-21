package chat

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"shardlands/pkg/es"
	"shardlands/services/world"
)

func testStore(t *testing.T) *es.Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-chat-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s, err := es.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func said(t *testing.T, s *es.Store, name, text string) {
	t.Helper()
	data, _ := json.Marshal(world.ChatSaid{PlayerID: "p-" + name, Name: name, Text: text})
	if _, err := s.Append(world.ChatStream, es.AnyVersion,
		es.EventData{Type: world.EventChatSaid, Data: data}); err != nil {
		t.Fatal(err)
	}
}

func waitRecent(t *testing.T, h *History, want int) []Message {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := h.Recent(0); len(got) == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("recent never reached %d messages (have %d)", want, len(h.Recent(0)))
	return nil
}

// Projection: mevcut event'leri catch-up ile, yenilerini sinyalle almalı;
// ilgisiz event'leri yok saymalı.
func TestHistoryCatchUpAndLive(t *testing.T) {
	s := testStore(t)
	said(t, s, "ayşe", "önceden yazılmıştı") // projection başlamadan önce
	s.Append("other", es.AnyVersion, es.EventData{Type: "Noise", Data: []byte(`{}`)})

	h := NewHistory(s)
	defer h.Close()

	got := waitRecent(t, h, 1) // catch-up
	if got[0].Name != "ayşe" || got[0].Text != "önceden yazılmıştı" {
		t.Fatalf("caught up = %+v", got[0])
	}

	said(t, s, "bora", "canlı mesaj") // canlı akış
	got = waitRecent(t, h, 2)
	if got[1].Name != "bora" {
		t.Fatalf("live message = %+v", got[1])
	}
}

// Kapasite: yalnızca son maxKeep mesaj tutulmalı (en eskiler düşer).
func TestHistoryTrimsToCapacity(t *testing.T) {
	s := testStore(t)
	h := NewHistory(s)
	defer h.Close()

	for i := 0; i < maxKeep+10; i++ {
		said(t, s, "x", fmt.Sprintf("m-%03d", i))
	}
	// Uzunluk trim yüzünden erken 100'e ulaşır; SON mesajın işlenmesini
	// bekle (uzunluğa bakmak yarışlıdır — bunu bu test bize öğretti).
	final := fmt.Sprintf("m-%03d", maxKeep+9)
	deadline := time.Now().Add(2 * time.Second)
	var got []Message
	for time.Now().Before(deadline) {
		got = h.Recent(0)
		if len(got) > 0 && got[len(got)-1].Text == final {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(got) != maxKeep || got[0].Text != "m-010" || got[len(got)-1].Text != final {
		t.Fatalf("window = %d [%s..%s], want %d [m-010..%s]",
			len(got), got[0].Text, got[len(got)-1].Text, maxKeep, final)
	}

	// Recent(n) son n'i vermeli.
	last2 := h.Recent(2)
	if len(last2) != 2 || last2[1].Text != got[len(got)-1].Text {
		t.Fatalf("Recent(2) = %+v", last2)
	}
}
