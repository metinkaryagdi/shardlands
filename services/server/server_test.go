package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"shardlands/services/world"
)

// tmpDir: Windows delete-pending esnekliği için en-iyi-çaba temizlikli
// geçici dizin (pkg/storage testlerindeki gerekçeyle aynı).
func tmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "shardlands-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func startTestServerAt(t *testing.T, dataDir string) *Server {
	t.Helper()
	srv, err := Start(Config{
		HTTPAddr:        "127.0.0.1:0",
		PlayerAddr:      "127.0.0.1:0",
		MatchmakingAddr: "127.0.0.1:0",
		Secret:          []byte("e2e-secret"),
		ClientDir:       tmpDir(t),
		DataDir:         dataDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

func startTestServer(t *testing.T) *Server {
	t.Helper()
	return startTestServerAt(t, tmpDir(t))
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
		ID     string  `json:"id"`
		Name   string  `json:"name"`
		X      float64 `json:"x"`
		Y      float64 `json:"y"`
		Bubble string  `json:"bubble"`
	} `json:"players"`
	Nodes []struct {
		ID        string  `json:"id"`
		Kind      string  `json:"kind"`
		X         float64 `json:"x"`
		Y         float64 `json:"y"`
		Available bool    `json:"available"`
	} `json:"nodes"`
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

// Chat dilimi: mesaj → diğer istemcide balon → read model endpoint'i →
// sunucu RESTART'ından sonra geçmiş hâlâ orada (event store kalıcılığı,
// tüm yığının içinden).
func TestChatBubbleHistoryAndRestart(t *testing.T) {
	dataDir := tmpDir(t)
	srv := startTestServerAt(t, dataDir)

	id1, tok1 := login(t, srv.HTTPAddr, "ayşe")
	_, tok2 := login(t, srv.HTTPAddr, "bora")
	ws1 := dialWS(t, srv.HTTPAddr, tok1)
	ws2 := dialWS(t, srv.HTTPAddr, tok2)
	readMsg(t, ws1) // welcome
	readMsg(t, ws2)

	if err := ws1.WriteJSON(map[string]any{"type": "chat", "text": "selam dünya"}); err != nil {
		t.Fatal(err)
	}

	// Bora'nın gözünden: ayşe'nin balonu görünmeli.
	deadline := time.Now().Add(5 * time.Second)
	seen := false
	for !seen && time.Now().Before(deadline) {
		m := readMsg(t, ws2)
		if m.Type != "snapshot" {
			continue
		}
		for _, p := range m.Players {
			if p.ID == id1 && p.Bubble == "selam dünya" {
				seen = true
			}
		}
	}
	if !seen {
		t.Fatal("bubble never appeared in other client's snapshots")
	}

	// Read model: /api/chat/recent (eventual consistency — kısa bekleme).
	waitChat := func(addr string) bool {
		d := time.Now().Add(3 * time.Second)
		for time.Now().Before(d) {
			resp, err := http.Get("http://" + addr + "/api/chat/recent")
			if err == nil {
				var msgs []struct{ Name, Text string }
				json.NewDecoder(resp.Body).Decode(&msgs)
				resp.Body.Close()
				for _, m := range msgs {
					if m.Name == "ayşe" && m.Text == "selam dünya" {
						return true
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		return false
	}
	if !waitChat(srv.HTTPAddr) {
		t.Fatal("chat message never reached read model")
	}

	// Restart: aynı DataDir ile yeni sunucu — projection event log'dan
	// sıfırdan kurulur, geçmiş kalıcıdır.
	ws1.Close()
	ws2.Close()
	srv.Stop()
	srv2 := startTestServerAt(t, dataDir)
	if !waitChat(srv2.HTTPAddr) {
		t.Fatal("chat history lost after server restart")
	}
}

// Kaynak dilimi: node'a yürü → E ile topla → node snapshot'ta tükenir →
// envanter read model'i /api/inventory'de görünür.
func TestGatherAndInventoryE2E(t *testing.T) {
	srv := startTestServer(t)
	id, tok := login(t, srv.HTTPAddr, "toplayıcı")
	ws := dialWS(t, srv.HTTPAddr, tok)
	readMsg(t, ws) // welcome

	// n5 (400,180)'e doğru yukarı yürü (spawn: 400,300).
	if err := ws.WriteJSON(map[string]any{"type": "input", "up": true}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	arrived := false
	for !arrived && time.Now().Before(deadline) {
		m := readMsg(t, ws)
		if m.Type != "snapshot" {
			continue
		}
		for _, p := range m.Players {
			if p.ID == id && p.Y <= 190 {
				arrived = true
			}
		}
	}
	if !arrived {
		t.Fatal("player never reached the node")
	}
	ws.WriteJSON(map[string]any{"type": "input"}) // dur
	ws.WriteJSON(map[string]any{"type": "gather"})

	// Node tükenmiş görünmeli.
	depleted := false
	for !depleted && time.Now().Before(deadline) {
		m := readMsg(t, ws)
		if m.Type != "snapshot" {
			continue
		}
		for _, n := range m.Nodes {
			if n.ID == "n5" && !n.Available {
				depleted = true
			}
		}
	}
	if !depleted {
		t.Fatal("node never depleted after gather")
	}

	// Envanter read model'i (eventual consistency — kısa bekleme).
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + srv.HTTPAddr + "/api/inventory?player=" + id)
		if err == nil {
			var inv map[string]int
			json.NewDecoder(resp.Body).Decode(&inv)
			resp.Body.Close()
			if inv["wood"] == 1 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("inventory read model never showed gathered wood")
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
