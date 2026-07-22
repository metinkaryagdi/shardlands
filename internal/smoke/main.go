// Küme duman testi (smoke test): kurulumun uçtan uca çalıştığını
// dışarıdan, gerçek bir istemci gibi doğrular.
//
//	go run ./internal/smoke                       # http://localhost:30080
//	BASE=http://localhost:8080 go run ./internal/smoke   # tek süreç
//
// Adımlar: iki oyuncu giriş yapar → WS'e bağlanır → ikisi de 1v1
// kuyruğuna girer. Beklenen zincir: matchmaking saga'sı Arena CRD'si
// yazar → operator Pod açar → gateway oturumları o Pod'a gRPC ile vekil
// eder → istemciye 30Hz "arena" kareleri düşer.
//
// Neden birim testi değil? Burada doğrulanan şey Go kodu değil,
// TOPOLOJİ: imaj, RBAC, DNS, Pod zamanlaması ve düğümler arası ağ.
// Bunların hiçbiri süreç içi testte kırılmaz.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	watch := flag.Duration("watch", 45*time.Second, "kuyruktan sonra izleme süresi")
	flag.Parse()

	base := os.Getenv("BASE")
	if base == "" {
		base = "http://localhost:30080"
	}

	var arenaFrames atomic.Int64
	var conns []*websocket.Conn
	for _, name := range []string{"kaan", "elif"} {
		tok, id := login(base, name)
		fmt.Printf("giriş: %-5s -> %s\n", name, id)
		c := dial(base, tok)
		conns = append(conns, c)
		go func(n string, c *websocket.Conn) {
			for {
				_, data, err := c.ReadMessage()
				if err != nil {
					return
				}
				var m struct {
					Type string `json:"type"`
				}
				json.Unmarshal(data, &m)
				switch m.Type {
				case "snapshot": // 20Hz hub akışı: sessiz say
				case "arena":
					arenaFrames.Add(1)
				default:
					fmt.Printf("[%-5s] %s\n", n, data)
				}
			}
		}(name, c)
	}
	time.Sleep(time.Second)

	for _, c := range conns {
		if err := c.WriteJSON(map[string]string{"type": "queue", "mode": "1v1"}); err != nil {
			log.Fatal(err)
		}
	}
	time.Sleep(*watch)
	for _, c := range conns {
		c.Close()
	}

	n := arenaFrames.Load()
	fmt.Printf("arena karesi: %d\n", n)
	if n == 0 {
		fmt.Println("BAŞARISIZ: arena karesi gelmedi")
		os.Exit(1)
	}
	fmt.Println("TAMAM")
}

func login(base, name string) (token, id string) {
	body, _ := json.Marshal(map[string]string{"name": name})
	resp, err := http.Post(base+"/api/login", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct{ PlayerId, Token string }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Fatal(err)
	}
	if out.Token == "" {
		log.Fatalf("giriş başarısız: %s", name)
	}
	return out.Token, out.PlayerId
}

func dial(base, token string) *websocket.Conn {
	url := "ws" + base[len("http"):] + "/ws?token=" + token
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal(err)
	}
	return c
}
