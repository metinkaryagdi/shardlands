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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "shardlands/gen/shardlands/v1"
	"shardlands/pkg/actor"
	"shardlands/pkg/auth"
	"shardlands/services/chat"
	"shardlands/services/inventory"
	"shardlands/services/trade"
	"shardlands/services/world"
)

type Config struct {
	Secret    []byte
	ClientDir string // statik istemci dosyaları (index.html)
	System    *actor.System
	World     *actor.Ref
	Players   pb.PlayerServiceClient
	Chat      *chat.History        // sohbet read model'i (sorgu tarafı)
	Inventory *inventory.Inventory // envanter read model'i
	Trades    *trade.Orchestrator  // takas saga koordinatörü
}

type Gateway struct {
	cfg      Config
	upgrader websocket.Upgrader
	tradeSeq atomic.Int64
}

// New, gateway'in http.Handler'ını kurar.
func New(cfg Config) http.Handler {
	g := &Gateway{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/login", g.handleLogin)
	mux.HandleFunc("GET /api/chat/recent", g.handleChatRecent)
	mux.HandleFunc("GET /api/inventory", g.handleInventory)
	mux.HandleFunc("POST /api/trade", g.handleTrade)
	mux.HandleFunc("GET /ws", g.handleWS)
	mux.Handle("/", noCache(http.FileServer(http.Dir(cfg.ClientDir))))
	return mux
}

// noCache, statik istemci dosyalarına no-cache başlığı ekler. Faz 1/2
// geliştirmesinde tarayıcının index.html'in eski bir sürümünü önbellekten
// servis etmesi (canlı doğrulamada başımıza geldi) böyle önlenir; üretimde
// varlık sürümleme (content hash) tercih edilir.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}

// handleChatRecent: CQRS sorgu tarafı — event log'a değil read model'e
// gider; world aktörüne hiç dokunmaz.
func (g *Gateway) handleChatRecent(w http.ResponseWriter, r *http.Request) {
	msgs := g.cfg.Chat.Recent(50)
	if msgs == nil {
		msgs = []chat.Message{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
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

// handleInventory: oyuncunun envanteri (read model'den).
func (g *Gateway) handleInventory(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("player")
	if playerID == "" {
		http.Error(w, "player query param required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(g.cfg.Inventory.Get(playerID))
}

// handleTrade: takas teklifini orkestratöre verir. Proposer, token'dan
// (kimlik doğrulanmış oyuncu) gelir; counterparty ve mallar gövdeden.
// Canlı yolda karşı taraf otomatik kabul eder (AutoAccept) — gerçek onay
// UX'i (karşı tarafın istemcisinde "kabul et" akışı) kapsam dışı.
func (g *Gateway) handleTrade(w http.ResponseWriter, r *http.Request) {
	claims, err := auth.Verify(g.cfg.Secret, r.URL.Query().Get("token"))
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body struct {
		Counterparty string `json:"counterparty"`
		GiveKind     string `json:"giveKind"`
		GiveAmount   int    `json:"giveAmount"`
		WantKind     string `json:"wantKind"`
		WantAmount   int    `json:"wantAmount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	offer := trade.Offer{
		ID:           g.newTradeID(),
		Proposer:     claims.Sub,
		Counterparty: body.Counterparty,
		Give:         trade.Item{Kind: body.GiveKind, Amount: body.GiveAmount},
		Want:         trade.Item{Kind: body.WantKind, Amount: body.WantAmount},
	}
	st, err := g.cfg.Trades.Execute(offer, trade.AutoAccept)
	if err != nil {
		log.Printf("gateway: trade execute: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":     offer.ID,
		"phase":  st.Phase.String(),
		"reason": st.Reason,
	})
}

func (g *Gateway) newTradeID() string {
	return "t" + strconv.FormatInt(g.tradeSeq.Add(1), 10)
}

// ---- WebSocket + session aktörü ----

// İstemci → sunucu mesajları: "input" (basılı tuş durumu) ve "chat".
type clientMsg struct {
	Type  string `json:"type"`
	Up    bool   `json:"up"`
	Down  bool   `json:"down"`
	Left  bool   `json:"left"`
	Right bool   `json:"right"`
	Text  string `json:"text"`
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
			switch {
			case json.Unmarshal(data, &m) != nil:
			case m.Type == "input" || m.Type == "chat" || m.Type == "gather":
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
			players[i] = map[string]any{"id": p.ID, "name": p.Name, "x": p.X, "y": p.Y, "bubble": p.Bubble}
		}
		nodes := make([]map[string]any, len(m.Nodes))
		for i, n := range m.Nodes {
			nodes[i] = map[string]any{"id": n.ID, "kind": n.Kind, "x": n.X, "y": n.Y, "available": n.Available}
		}
		s.write(ctx, map[string]any{"type": "snapshot", "tick": m.Tick, "players": players, "nodes": nodes})
	case clientMsg:
		switch m.Type {
		case "chat":
			ctx.Send(s.world, world.Chat{PlayerID: s.id, Text: m.Text})
		case "gather":
			ctx.Send(s.world, world.Gather{PlayerID: s.id})
		default: // "input"
			ctx.Send(s.world, world.Input{
				PlayerID: s.id,
				Up:       m.Up, Down: m.Down, Left: m.Left, Right: m.Right,
			})
		}
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
