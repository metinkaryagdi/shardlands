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
	"errors"
	"log"
	"net"
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
	"shardlands/pkg/ratelimit"
	"shardlands/pkg/resilience"
	"shardlands/services/chat"
	"shardlands/services/inventory"
	"shardlands/services/stats"
	"shardlands/services/trade"
	"shardlands/services/world"
)

type Config struct {
	Secret    []byte
	ClientDir string // statik istemci dosyaları (index.html)
	System    *actor.System
	Router    *world.Router
	Players   pb.PlayerServiceClient
	Chat      *chat.History        // sohbet read model'i (sorgu tarafı)
	Inventory *inventory.Inventory // envanter read model'i
	Trades    *trade.Orchestrator  // takas saga koordinatörü
	Stats     *stats.Stats         // global sayaçlar (CRDT)
}

type Gateway struct {
	cfg      Config
	upgrader websocket.Upgrader
	tradeSeq atomic.Int64
	online   atomic.Int64 // anlık çevrimiçi oyuncu (basit gauge; CRDT değil)

	// playerGuard, player-service çağrılarını korur: bulkhead
	// (eşzamanlılık bütçesi) + circuit breaker (bozuk bağımlılığa
	// yüklenmeme). Kaskad arızayı gateway sınırında keser.
	playerGuard resilience.Guard

	// loginLimiter, IP başına giriş hızını sınırlar (kötüye kullanım).
	loginLimiter *ratelimit.Keyed
	// shed, hız sınırı yüzünden atılan komut sayısı (gözlem).
	shed atomic.Int64
}

// New, gateway'in http.Handler'ını kurar.
func New(cfg Config) http.Handler {
	g := &Gateway{cfg: cfg}
	g.playerGuard = resilience.Guard{
		Breaker: resilience.NewBreaker(resilience.BreakerOptions{
			FailureThreshold: 5,
			OpenDuration:     5 * time.Second,
		}),
		Bulkhead: resilience.NewBulkhead(32, 0),
	}
	// Giriş: IP başına dakikada ~60, anlık 10'luk patlamaya izin.
	g.loginLimiter = ratelimit.NewKeyed(ratelimit.Options{Rate: 1, Burst: 10})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/login", g.handleLogin)
	mux.HandleFunc("GET /api/chat/recent", g.handleChatRecent)
	mux.HandleFunc("GET /api/inventory", g.handleInventory)
	mux.HandleFunc("GET /api/stats", g.handleStats)
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
	if !g.loginLimiter.Allow(clientIP(r)) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Çağrıyı koruma altında yap. İSTEMCİ HATASI (geçersiz isim) devre
	// kesici için BAŞARI sayılır: bağımlılık sağlıklı, kullanıcı yanlış.
	// Aksi halde birkaç boş isim denemesi devreyi açar ve sağlıklı bir
	// servisi cezalandırırdık.
	var (
		resp     *pb.CreatePlayerResponse
		clientEr error
	)
	err := g.playerGuard.Do(r.Context(), func() error {
		out, e := g.cfg.Players.CreatePlayer(r.Context(), &pb.CreatePlayerRequest{Name: body.Name})
		if e != nil && status.Code(e) == codes.InvalidArgument {
			clientEr = e
			return nil
		}
		resp = out
		return e
	})
	switch {
	case clientEr != nil:
		http.Error(w, status.Convert(clientEr).Message(), http.StatusBadRequest)
		return
	case errors.Is(err, resilience.ErrOpen), errors.Is(err, resilience.ErrFull):
		// Yük atma / hızlı başarısızlık: istemciye "sonra dene" de.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "service busy, retry", http.StatusServiceUnavailable)
		return
	case err != nil:
		log.Printf("gateway: create player: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
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

// handleStats: global sayaçlar. toplam toplanan CRDT G-Counter'dan
// (Faz 3'te shard'lar arası merge ile yakınsar), çevrimiçi anlık gauge'dan.
func (g *Gateway) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"totalGathered": g.cfg.Stats.TotalGathered(),
		"online":        g.online.Load(),
		"shedded":       g.shed.Load(), // hız sınırıyla atılan komut sayısı
	})
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

// clientIP, hız sınırı anahtarı (port'suz uzak adres).
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
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
			return &session{conn: conn, router: g.cfg.Router, id: claims.Sub, name: claims.Name}
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

	// Çevrimiçi gauge'i process yaşam döngüsüne bağla (restart-güvenli:
	// Stopped process tamamen durunca bir kez ateşlenir, aktör
	// instance'ının restart'ından etkilenmez).
	g.online.Add(1)
	go func() {
		<-ref.Stopped()
		g.online.Add(-1)
	}()

	// Bağlantı başına komut hız sınırı: taşkın bir istemci dünyayı
	// meşgul edemesin. Fazlası DÜŞÜRÜLÜR (load shedding) — girdi
	// durum-tabanlı olduğundan bir komutu atmak zararsızdır; bir
	// sonraki durum değişikliği zaten yeni komut üretir.
	limiter := ratelimit.New(ratelimit.Options{Rate: 50, Burst: 100})

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
				if !limiter.Allow() {
					g.shed.Add(1)
					continue
				}
				ref.Send(m)
			}
		}
	}()
}

// session, WS bağlantısının aktörüdür. Faz 3'te oyuncu bir BÖLGEYE aittir
// ve region ref'i handoff ile değişir; session bu ref'i takip eder ve
// input'u güncel bölgeye yönlendirir.
type session struct {
	conn     *websocket.Conn
	router   *world.Router
	region   *actor.Ref // güncel bölge (handoff'ta güncellenir)
	regionID string
	shard    string
	id, name string
}

func (s *session) PreStart(ctx *actor.Context) {
	// Merkeze yakın bir noktada doğ; bölgeyi router seçer.
	rid, shard, ref := s.router.SpawnRegion(world.Width/2, world.Height/2)
	s.region, s.regionID, s.shard = ref, rid, shard
	ctx.Send(ref, world.Join{
		PlayerID: s.id, Name: s.name, Session: ctx.Self(),
		X: world.Width / 2, Y: world.Height / 2,
	})
	s.write(ctx, map[string]any{
		"type": "welcome", "id": s.id, "name": s.name,
		"w": world.Width, "h": world.Height, "tickRate": world.TickRate,
		"cols": world.Cols, "rows": world.Rows,
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
		s.write(ctx, map[string]any{"type": "snapshot", "tick": m.Tick,
			"region": m.RegionID, "shard": m.Shard, "players": players, "nodes": nodes})
	case world.AssignedRegion:
		// Handoff: bundan sonra input yeni bölgeye gider.
		s.region, s.regionID, s.shard = m.Ref, m.RegionID, m.Shard
	case clientMsg:
		switch m.Type {
		case "chat":
			ctx.Send(s.region, world.Chat{PlayerID: s.id, Text: m.Text})
		case "gather":
			ctx.Send(s.region, world.Gather{PlayerID: s.id})
		default: // "input"
			ctx.Send(s.region, world.Input{
				PlayerID: s.id,
				Up:       m.Up, Down: m.Down, Left: m.Left, Right: m.Right,
			})
		}
	}
}

// PostStop: bağlantı hangi yoldan koparsa kopsun güncel bölgeden ayrıl ve
// soketi kapat. Leave idempotent; handoff sırasında kopsa bile en fazla
// eski bölgeye zararsız bir Leave gider.
func (s *session) PostStop(ctx *actor.Context) {
	ctx.Send(s.region, world.Leave{PlayerID: s.id})
	s.conn.Close()
}

func (s *session) write(ctx *actor.Context, v any) {
	s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := s.conn.WriteJSON(v); err != nil {
		ctx.Self().Stop() // yazamıyorsak oturum ölüdür
	}
}
