// Package gateway, BFF'dir (Backend-for-Frontend): istemcinin tek
// kapısı. İçeride gRPC konuşan servisleri, dışarıda tarayıcının
// anladığı dile (HTTP+JSON, WebSocket) çevirir. İstemci hiçbir iç
// servisi doğrudan görmez — servis topolojisi değişse de (Faz 3'te
// shard'lar, Faz 4'te event bus) istemci sözleşmesi sabit kalır.
//
// Her WS bağlantısı bir SESSION AKTÖRÜDÜR: dünyadan gelen Snapshot'lar
// ve istemciden gelen girdiler aynı mailbox'ta buluşur, WS yazmaları
// tek goroutine'den (aktörün kendisi) yapılır — gorilla/websocket'in
// "tek yazar" kuralı kendiliğinden sağlanır. Yavaş istemci dünyayı
// YAVAŞLATAMAZ: session mailbox'ı DropNewest'tir, dolarsa kareler
// düşer (taze kare zaten yoldadır; load shedding'in en küçük hali).
package gateway

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/actor"
	"shardlands/pkg/auth"
	"shardlands/services/world"
)

type Config struct {
	Secret    []byte
	ClientDir string // statik istemci dosyaları (index.html)
	System    *actor.System
	World     *actor.Ref
	Players   pb.PlayerServiceClient
}

type Gateway struct {
	cfg      Config
	upgrader websocket.Upgrader
}

// New, gateway'in http.Handler'ını kurar.
func New(cfg Config) http.Handler {
	g := &Gateway{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/login", g.handleLogin)
	mux.HandleFunc("GET /ws", g.handleWS)
	mux.Handle("/", http.FileServer(http.Dir(cfg.ClientDir)))
	return mux
}

// ---- kimlik ----

func (g *Gateway) handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	resp, err := g.cfg.Players.CreatePlayer(r.Context(), &pb.CreatePlayerRequest{Name: body.Name})
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			http.Error(w, status.Convert(err).Message(), http.StatusBadRequest)
		} else {
			log.Printf("gateway: create player: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"playerId": resp.PlayerId,
		"token":    resp.Token,
	})
}

// ---- WebSocket + session aktörü ----

// İstemci → sunucu tek mesaj tipi (Faz 1): basılı tuş durumu.
type clientMsg struct {
	Type  string `json:"type"`
	Up    bool   `json:"up"`
	Down  bool   `json:"down"`
	Left  bool   `json:"left"`
	Right bool   `json:"right"`
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	claims, err := auth.Verify(g.cfg.Secret, r.URL.Query().Get("token"))
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := g.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade cevabı kendisi yazdı
	}

	ref, err := g.cfg.System.Spawn(actor.Props{
		Name:        "session-" + claims.Sub,
		MailboxSize: 16,
		Overflow:    actor.DropNewest, // yavaş istemci: kare düşür, dünyayı bekletme
		Producer: func() actor.Actor {
			return &session{conn: conn, world: g.cfg.World, id: claims.Sub, name: claims.Name}
		},
	})
	if err != nil {
		// Aynı oyuncunun ikinci bağlantısı (isim çakışması) veya kapanış.
		conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "already connected"),
			time.Now().Add(time.Second))
		conn.Close()
		return
	}

	// Okuyucu goroutine: WS'den gelen her şey aktörün mailbox'ına.
	// Bağlantı kopunca (hata) aktörü durdurur; Leave, PostStop'ta.
	go func() {
		defer ref.Stop()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m clientMsg
			if json.Unmarshal(data, &m) == nil && m.Type == "input" {
				ref.Send(m)
			}
		}
	}()
}

type session struct {
	conn     *websocket.Conn
	world    *actor.Ref
	id, name string
}

func (s *session) PreStart(ctx *actor.Context) {
	ctx.Send(s.world, world.Join{PlayerID: s.id, Name: s.name, Session: ctx.Self()})
	s.write(ctx, map[string]any{
		"type": "welcome", "id": s.id, "name": s.name,
		"w": world.Width, "h": world.Height, "tickRate": world.TickRate,
	})
}

func (s *session) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case world.Snapshot:
		players := make([]map[string]any, len(m.Players))
		for i, p := range m.Players {
			players[i] = map[string]any{"id": p.ID, "name": p.Name, "x": p.X, "y": p.Y}
		}
		s.write(ctx, map[string]any{"type": "snapshot", "tick": m.Tick, "players": players})
	case clientMsg:
		ctx.Send(s.world, world.Input{
			PlayerID: s.id,
			Up:       m.Up, Down: m.Down, Left: m.Left, Right: m.Right,
		})
	}
}

// PostStop: bağlantı hangi yoldan koparsa kopsun (okuma hatası, yazma
// hatası, sistem kapanışı) dünyadan ayrıl ve soketi kapat — tek çıkış
// noktası. Leave idempotent olduğu için çifte tetiklenme zararsız.
func (s *session) PostStop(ctx *actor.Context) {
	ctx.Send(s.world, world.Leave{PlayerID: s.id})
	s.conn.Close()
}

func (s *session) write(ctx *actor.Context, v any) {
	s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := s.conn.WriteJSON(v); err != nil {
		ctx.Self().Stop() // yazamıyorsak oturum ölüdür
	}
}
