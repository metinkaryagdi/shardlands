package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"shardlands/services/world"
)

func startTestServer(t *testing.T) *Server {
	t.Helper()
	srv, err := Start(Config{
		HTTPAddr:        "127.0.0.1:0",
		PlayerAddr:      "127.0.0.1:0",
		MatchmakingAddr: "127.0.0.1:0",
		Secret:          []byte("e2e-secret"),
		ClientDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

func login(t *testing.T, addr, name string) (id, token string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"name": name})
	resp, err := http.Post("http://"+addr+"/api/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d", resp.StatusCode)
	}
	var out struct{ PlayerId, Token string }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.PlayerId, out.Token
}

func dialWS(t *testing.T, addr, token string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/ws?token=%s", addr, token), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

type wireMsg struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Tick    uint64 `json:"tick"`
	Players []struct {
		ID   string  `json:"id"`
		Name string  `json:"name"`
		X    float64 `json:"x"`
		Y    float64 `json:"y"`
	} `json:"players"`
}

func readMsg(t *testing.T, c *websocket.Conn) wireMsg {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var m wireMsg
	if err := c.ReadJSON(&m); err != nil {
		t.Fatalf("read: %v", err)
	}
	return m
}

// Faz 1'in uçtan uca dilimi: iki istemci aynı hub'da — biri hareket
// eder, İKİSİ DE hareketi görür.
func TestTwoClientsSeeEachOtherMoving(t *testing.T) {
	srv := startTestServer(t)

	id1, tok1 := login(t, srv.HTTPAddr, "ayşe")
	id2, tok2 := login(t, srv.HTTPAddr, "bora")
	ws1 := dialWS(t, srv.HTTPAddr, tok1)
	ws2 := dialWS(t, srv.HTTPAddr, tok2)

	if m := readMsg(t, ws1); m.Type != "welcome" || m.ID != id1 {
		t.Fatalf("ws1 first msg = %+v, want welcome/%s", m, id1)
	}
	if m := readMsg(t, ws2); m.Type != "welcome" || m.ID != id2 {
		t.Fatalf("ws2 first msg = %+v, want welcome/%s", m, id2)
	}

	// Oyuncu 1 sağa basılı tutuyor.
	if err := ws1.WriteJSON(map[string]any{"type": "input", "right": true}); err != nil {
		t.Fatal(err)
	}

	// Oyuncu 2'nin gözünden: iki oyuncu da görünmeli ve p1 sağa
	// ilerlemiş olmalı (başlangıç: merkez).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m := readMsg(t, ws2)
		if m.Type != "snapshot" {
			continue
		}
		var x1 float64
		seen := map[string]bool{}
		for _, p := range m.Players {
			seen[p.ID] = true
			if p.ID == id1 {
				x1 = p.X
			}
		}
		if seen[id1] && seen[id2] && x1 > world.Width/2+20 {
			// Oyuncu 1 de aynı dünyayı görüyor olmalı.
			for time.Now().Before(deadline) {
				m1 := readMsg(t, ws1)
				if m1.Type == "snapshot" && len(m1.Players) == 2 {
					return // dilim tamam
				}
			}
		}
	}
	t.Fatal("clients never saw each other moving")
}

// Kimliksiz/bozuk token'la WS el sıkışması reddedilmeli.
func TestWSRejectsBadToken(t *testing.T) {
	srv := startTestServer(t)
	_, resp, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/ws?token=garbage", srv.HTTPAddr), nil)
	if err == nil {
		t.Fatal("dial with bad token must fail")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, want 401", resp)
	}
}

// Geçersiz isim gateway'de 400'e çevrilmeli (gRPC InvalidArgument → HTTP).
func TestLoginValidationMapsToHTTP400(t *testing.T) {
	srv := startTestServer(t)
	body, _ := json.Marshal(map[string]string{"name": "   "})
	resp, err := http.Post("http://"+srv.HTTPAddr+"/api/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// Bağlantı kopunca oyuncu dünyadan düşmeli: kalan istemci onu görmemeli.
func TestDisconnectRemovesPlayer(t *testing.T) {
	srv := startTestServer(t)

	_, tok1 := login(t, srv.HTTPAddr, "kalan")
	id2, tok2 := login(t, srv.HTTPAddr, "giden")
	ws1 := dialWS(t, srv.HTTPAddr, tok1)
	ws2 := dialWS(t, srv.HTTPAddr, tok2)
	readMsg(t, ws1) // welcome
	readMsg(t, ws2)

	// Önce ikisinin de göründüğünden emin ol.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := readMsg(t, ws1); m.Type == "snapshot" && len(m.Players) == 2 {
			break
		}
	}

	ws2.Close()

	for time.Now().Before(deadline) {
		m := readMsg(t, ws1)
		if m.Type != "snapshot" {
			continue
		}
		gone := true
		for _, p := range m.Players {
			if p.ID == id2 {
				gone = false
			}
		}
		if gone && len(m.Players) == 1 {
			return
		}
	}
	t.Fatal("disconnected player never removed from snapshots")
}
