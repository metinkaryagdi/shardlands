package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"shardlands/services/handoff"
)

// arenaMsg, arena moduna ait tel mesajları.
type arenaMsg struct {
	Type        string `json:"type"`
	ArenaID     string `json:"arenaId"`
	Team        int    `json:"team"`
	Mode        string `json:"mode"`
	Region      string `json:"region"`
	Over        bool   `json:"over"`
	WinnerTeam  int    `json:"winnerTeam"`
	RemainingMs int64  `json:"remainingMs"`
	Players     []struct {
		ID     string  `json:"id"`
		Team   int     `json:"team"`
		X      float64 `json:"x"`
		Y      float64 `json:"y"`
		Health int     `json:"health"`
		Alive  bool    `json:"alive"`
	} `json:"players"`
}

// readerFor, WS'i arka planda okuyup arena/hub mesajlarını kanala akıtır.
func readerFor(ws *websocket.Conn) <-chan arenaMsg {
	ch := make(chan arenaMsg, 512)
	go func() {
		defer close(ch)
		for {
			ws.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var m arenaMsg
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			select {
			case ch <- m:
			default:
			}
		}
	}()
	return ch
}

// waitType, verilen tipte mesaj bekler.
func waitType(t *testing.T, ch <-chan arenaMsg, typ string, d time.Duration) arenaMsg {
	t.Helper()
	timeout := time.After(d)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				t.Fatalf("connection closed while waiting for %q", typ)
			}
			if m.Type == typ {
				return m
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %q", typ)
		}
	}
}

// Faz 5 dikey dilimi: iki oyuncu 1v1 kuyruğuna girer → matchmaking saga
// arenayı açar → handoff (dlock + fencing) oyuncuları arenaya taşır →
// maç biter → oyuncular hub'a döner. Her adımın denetim izi yazılır.
func TestArenaMatchAndHandoffE2E(t *testing.T) {
	srv := startTestServer(t)

	id1, tok1 := login(t, srv.HTTPAddr, "dövüşçü1")
	id2, tok2 := login(t, srv.HTTPAddr, "dövüşçü2")
	ws1 := dialWS(t, srv.HTTPAddr, tok1)
	ws2 := dialWS(t, srv.HTTPAddr, tok2)
	ch1, ch2 := readerFor(ws1), readerFor(ws2)

	// İkisi de hub'da başlar (welcome).
	waitType(t, ch1, "welcome", 5*time.Second)
	waitType(t, ch2, "welcome", 5*time.Second)

	// Kuyruğa gir: ikinci oyuncu girince saga tetiklenir.
	if err := ws1.WriteJSON(map[string]any{"type": "queue", "mode": "1v1"}); err != nil {
		t.Fatal(err)
	}
	waitType(t, ch1, "queued", 5*time.Second)
	if err := ws2.WriteJSON(map[string]any{"type": "queue", "mode": "1v1"}); err != nil {
		t.Fatal(err)
	}

	// HANDOFF: ikisi de arenaya alınmalı.
	a1 := waitType(t, ch1, "arena-enter", 10*time.Second)
	a2 := waitType(t, ch2, "arena-enter", 10*time.Second)
	if a1.ArenaID == "" || a1.ArenaID != a2.ArenaID {
		t.Fatalf("arena ids differ: %q vs %q", a1.ArenaID, a2.ArenaID)
	}
	if a1.Team == a2.Team {
		t.Fatalf("both players on team %d", a1.Team)
	}

	// Arena kareleri akmalı.
	frame := waitType(t, ch1, "arena", 10*time.Second)
	if len(frame.Players) != 2 {
		t.Fatalf("arena frame has %d players, want 2", len(frame.Players))
	}

	// Denetim izi: her iki oyuncu için hub'dan ayrılma + arenaya giriş.
	for _, id := range []string{id1, id2} {
		types := auditTypes(t, srv, id)
		if !contains(types, handoff.EventLeftHub) || !contains(types, handoff.EventEnteredArena) {
			t.Fatalf("audit trail for %s = %v, want LeftHub+EnteredArena", id, types)
		}
	}

	// Maçı bitir: bir oyuncu ayrılırsa diğeri kazanır.
	ws2.Close()

	// Kalan oyuncu hub'a dönmeli.
	hub := waitType(t, ch1, "hub-enter", 20*time.Second)
	if hub.Region == "" {
		t.Fatal("hub-enter without region")
	}
	// Denetim izi tamamlanmalı.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		types := auditTypes(t, srv, id1)
		if contains(types, handoff.EventLeftArena) && contains(types, handoff.EventEnteredHub) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("audit trail for %s never completed: %v", id1, auditTypes(t, srv, id1))
}

func auditTypes(t *testing.T, srv *Server, playerID string) []string {
	t.Helper()
	evs, err := srv.events.ReadStream(handoff.Stream(playerID), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
