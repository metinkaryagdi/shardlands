package chat

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"shardlands/internal/testenv"
	"shardlands/services/world"
)

func said(t *testing.T, env *testenv.Env, name, text string) {
	t.Helper()
	data, _ := json.Marshal(world.ChatSaid{PlayerID: "p-" + name, Name: name, Text: text})
	env.Append(t, world.ChatStream, world.EventChatSaid, data)
}

func waitRecent(t *testing.T, h *History, want int) []Message {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if got := h.Recent(0); len(got) == want {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("recent never reached %d messages (have %d)", want, len(h.Recent(0)))
	return nil
}

// Projection bus'tan tüketir: abonelik ÖNCESİ yazılanları da (akış
// baştan oynatıldığı için) ve sonrakileri de görür; ilgisiz event'leri
// yok sayar.
func TestHistoryCatchUpAndLive(t *testing.T) {
	env := testenv.New(t)
	said(t, env, "ayşe", "önceden yazılmıştı")
	env.Append(t, "other", "Noise", []byte(`{}`))
	env.WaitDelivered(t)

	h, err := NewHistory(env.Bus)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	got := waitRecent(t, h, 1) // baştan oynatma (catch-up)
	if got[0].Name != "ayşe" || got[0].Text != "önceden yazılmıştı" {
		t.Fatalf("caught up = %+v", got[0])
	}

	said(t, env, "bora", "canlı mesaj") // canlı akış
	got = waitRecent(t, h, 2)
	if got[1].Name != "bora" {
		t.Fatalf("live message = %+v", got[1])
	}
}

// Kapasite: yalnızca son maxKeep mesaj tutulur.
func TestHistoryTrimsToCapacity(t *testing.T) {
	env := testenv.New(t)
	h, err := NewHistory(env.Bus)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for i := 0; i < maxKeep+10; i++ {
		said(t, env, "x", fmt.Sprintf("m-%03d", i))
	}

	final := fmt.Sprintf("m-%03d", maxKeep+9)
	deadline := time.Now().Add(30 * time.Second)
	var got []Message
	for time.Now().Before(deadline) {
		got = h.Recent(0)
		if len(got) > 0 && got[len(got)-1].Text == final {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(got) != maxKeep || got[0].Text != "m-010" || got[len(got)-1].Text != final {
		t.Fatalf("window = %d [%s..%s], want %d [m-010..%s]",
			len(got), got[0].Text, got[len(got)-1].Text, maxKeep, final)
	}

	if last2 := h.Recent(2); len(last2) != 2 || last2[1].Text != final {
		t.Fatalf("Recent(2) = %+v", last2)
	}
}
