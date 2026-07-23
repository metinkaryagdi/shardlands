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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "shardlands/gen/shardlands/v1"
)

func main() {
	watch := flag.Duration("watch", 45*time.Second, "kuyruktan sonra izleme süresi")
	rogue := flag.String("rogue", "", "zero-trust testi: player servisini yetkisiz çağır")
	hammer := flag.Duration("hammer", 0, "kesintisizlik testi: bu süre boyunca sürekli giriş yap")
	// 1sn: gateway'in hız sınırlayıcısı IP başına saniyede 1 token
	// dolduruyor (pkg/ratelimit, burst 10). Daha sık vurmak dağıtım
	// kesintisini değil KENDİ YÜK ATMAMIZI ölçerdi — ilk denemede tam
	// olarak bu oldu: 200ms aralıkla istekler 429'a çarptı ve araç
	// "kesinti var" dedi. Ölçüm aracının ölçtüğü şeyi bilmesi gerekir.
	every := flag.Duration("every", time.Second, "giriş denemeleri arası süre")
	flag.Parse()

	base := os.Getenv("BASE")
	if base == "" {
		base = "http://localhost:30080"
	}

	if *rogue != "" {
		runRogue(*rogue)
		return
	}
	if *hammer > 0 {
		runHammer(base, *hammer, *every)
		return
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

// runHammer, KESİNTİSİZLİK DENEYİ. Verilen süre boyunca aralıksız
// /api/login çağırır ve başarısızlıkları sayar.
//
// Neden giriş? Çünkü zincirin tamamını geçer: gateway → (mesh, mTLS) →
// player Pod'u → token imzası. Yani hem hub'ın hem player'ın yeniden
// dağıtımını görür.
//
// Kullanım: bunu koştururken başka bir kabukta
//
//	kubectl -n shardlands rollout restart deployment/player
//
// Beklenen: player için 0 hata (2 kopya, maxUnavailable=0, preStop).
// Hub yeniden başlatılırsa hata BEKLENİR — tek kopya, kesintisiz
// olamaz. Deneyin değeri ikisini ayırt edebilmesinde.
func runHammer(base string, d, every time.Duration) {
	var ok, fail, shed int
	var firstErr string
	deadline := time.Now().Add(d)
	client := &http.Client{Timeout: 5 * time.Second}
	tick := time.NewTicker(every)
	defer tick.Stop()

	fmt.Printf("%s adresine %s boyunca her %s'de bir giriş...\n", base, d, every)
	for time.Now().Before(deadline) {
		<-tick.C
		body, _ := json.Marshal(map[string]string{"name": "hammer"})
		resp, err := client.Post(base+"/api/login", "application/json", bytes.NewReader(body))
		switch {
		case err != nil:
			// Bağlantı kurulamadı: dağıtım boşluğunun gerçek belirtisi.
			fail++
			if firstErr == "" {
				firstErr = err.Error()
			}
		case resp.StatusCode == http.StatusTooManyRequests:
			// 429 ARIZA DEĞİL. Faz 4'te bilerek yazdığımız yük atma
			// devrede demektir — sistem çalışıyor, bizi kısıyor.
			// Ayrı sayılmazsa ölçüm "kesinti" diye yalan söyler.
			shed++
			resp.Body.Close()
		case resp.StatusCode != http.StatusOK:
			fail++
			if firstErr == "" {
				firstErr = resp.Status
			}
			resp.Body.Close()
		default:
			ok++
			resp.Body.Close()
		}
	}

	fmt.Printf("başarılı: %d  başarısız: %d  hız sınırı (429): %d\n", ok, fail, shed)
	if fail > 0 {
		fmt.Printf("ilk hata: %s\n", firstErr)
		fmt.Println("KESİNTİ VAR")
		os.Exit(1)
	}
	fmt.Println("KESİNTİSİZ")
}

// runRogue, ZERO TRUST DENEYİ. Küme içinde ama BAŞKA bir
// ServiceAccount ile koşan bir Pod'dan player servisini çağırır.
//
// Beklenen: bağlantı reddedilir. Bu Pod player Service'inin IP'sine
// ulaşabilir — ağ onu engellemez, hatta aynı namespace'tedir. Engelleyen
// şey KİMLİKTİR: AuthorizationPolicy yalnız hub'ın ServiceAccount
// kimliğine izin veriyor. "Ağda olmak yetki üretmez" cümlesinin
// çalışan kanıtı budur.
//
// Testin ters mantığına dikkat: BAŞARI = çağrının başarısız olması.
// Çağrı geçerse politika delik demektir ve çıkış kodu 1'dir.
func runRogue(addr string) {
	fmt.Printf("yetkisiz çağrı deneniyor: %s\n", addr)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("TAMAM: bağlantı kurulamadı: %v\n", err)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := pb.NewPlayerServiceClient(conn).CreatePlayer(ctx,
		&pb.CreatePlayerRequest{Name: "sizma"})
	if err != nil {
		fmt.Printf("TAMAM: reddedildi -> %v\n", err)
		return
	}
	fmt.Printf("BAŞARISIZ: politika delik, token alındı: %s\n", resp.PlayerId)
	os.Exit(1)
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
