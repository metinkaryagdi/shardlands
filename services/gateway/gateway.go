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
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
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
	"shardlands/services/arena"
	"shardlands/services/chat"
	"shardlands/services/handoff"
	"shardlands/services/inventory"
	"shardlands/services/matchmaking"
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
	Matcher   *matchmaking.Matcher // arena kuyruğu (Faz 5)
	Handoff   *handoff.Coordinator // hub↔arena transferi (Faz 5)
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

	// sessions, playerID → oturum aktörü. Handoff koordinatörü ve
	// matchmaking oyuncuya buradan ulaşır.
	sessMu   sync.RWMutex
	sessions map[string]*actor.Ref

	mux http.Handler
}

// New, gateway'i kurar. *Gateway döner (yalnız http.Handler değil):
// handoff koordinatörü onu SessionPort, matchmaking ise Assigner olarak
// kullanır — bu döngüsel bağımlılık, gateway önce kurulup bağımlılıklar
// sonradan takılarak çözülür (SetHandoff).
func New(cfg Config) *Gateway {
	g := &Gateway{cfg: cfg, sessions: map[string]*actor.Ref{}}
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
	g.mux = mux
	return g
}

// Handler, HTTP yönlendiricisi.
func (g *Gateway) Handler() http.Handler { return g.mux }

// SetHandoff, handoff koordinatörünü sonradan takar (kurulum döngüsü).
func (g *Gateway) SetHandoff(c *handoff.Coordinator) { g.cfg.Handoff = c }

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

// İstemci → sunucu mesajları. Hub modunda "input"/"chat"/"gather",
// arena modunda "input"/"fire"; her iki modda "queue" (arena kuyruğu).
type clientMsg struct {
	Type  string  `json:"type"`
	Up    bool    `json:"up"`
	Down  bool    `json:"down"`
	Left  bool    `json:"left"`
	Right bool    `json:"right"`
	Text  string  `json:"text"`
	AimX  float64 `json:"aimX"`
	AimY  float64 `json:"aimY"`
	Mode  string  `json:"mode"`
}

// Oturuma gönderilen handoff emirleri. Cevap kanalı taşırlar: aktör
// framework'ünde ask deseni yok (Faz 1'de bilinçli olarak ertelendi),
// bu yüzden yanıt kanalı mesajın içinde gelir.
type enterArenaMsg struct {
	a     *arena.Arena
	team  int
	token uint64
	reply chan error
}

type enterHubMsg struct {
	token uint64
	reply chan error
}

// errStaleToken: fencing — daha yeni bir transfer zaten uygulanmış.
var errStaleToken = errors.New("gateway: stale handoff token")

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
			return &session{conn: conn, g: g, router: g.cfg.Router, id: claims.Sub, name: claims.Name}
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

	// Oturum kayıt defteri + matchmaking kaydı: handoff ve eşleştirme
	// oyuncuya buradan ulaşır.
	g.sessMu.Lock()
	g.sessions[claims.Sub] = ref
	g.sessMu.Unlock()
	if g.cfg.Matcher != nil {
		g.cfg.Matcher.Register(matchmaking.QueuedPlayer{ID: claims.Sub, Name: claims.Name, Session: ref})
	}

	// Çevrimiçi gauge'i process yaşam döngüsüne bağla (restart-güvenli:
	// Stopped process tamamen durunca bir kez ateşlenir, aktör
	// instance'ının restart'ından etkilenmez).
	g.online.Add(1)
	go func() {
		<-ref.Stopped()
		g.online.Add(-1)
		g.sessMu.Lock()
		delete(g.sessions, claims.Sub)
		g.sessMu.Unlock()
		if g.cfg.Matcher != nil {
			g.cfg.Matcher.Unregister(claims.Sub)
		}
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
			case m.Type == "input" || m.Type == "chat" || m.Type == "gather" ||
				m.Type == "fire" || m.Type == "queue":
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
	g        *Gateway
	router   *world.Router
	region   *actor.Ref // güncel bölge (hub modunda)
	regionID string
	shard    string
	id, name string

	// Arena modu: doluysa oyuncu hub'da DEĞİL, arenadadır.
	arena *arena.Arena
	team  int
	// token, son uygulanan handoff'un fencing token'ı. Daha küçük
	// token'lı emir REDDEDİLİR (gecikmiş/bayat transfer).
	token uint64
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
	case enterArenaMsg:
		s.onEnterArena(ctx, m)
	case enterHubMsg:
		s.onEnterHub(ctx, m)
	case arena.Snapshot:
		// Bayat arena karesi (eski maç) yok sayılır.
		if s.arena == nil || m.ArenaID != s.arena.ID() {
			return
		}
		s.write(ctx, map[string]any{
			"type": "arena", "arenaId": m.ArenaID, "tick": m.Tick,
			"remainingMs": m.RemainingMs, "players": m.Players,
			"projectiles": m.Projectiles, "over": m.Over, "winnerTeam": m.WinnerTeam,
			"you": s.id, "team": s.team,
		})
	case world.Snapshot:
		if s.arena != nil {
			return // arena modundayken hub kareleri gönderilmez
		}
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
		s.onClientMsg(ctx, m)
	}
}

// onClientMsg, komutu MODA göre yönlendirir: arenadaysak arena kuyruğuna
// (kilitsiz push), değilsek bölge aktörüne.
func (s *session) onClientMsg(ctx *actor.Context, m clientMsg) {
	if m.Type == "queue" {
		if s.g != nil && s.g.cfg.Matcher != nil && s.arena == nil {
			if _, err := s.g.cfg.Matcher.Enqueue(s.id, m.Mode); err != nil {
				s.write(ctx, map[string]any{"type": "queue-error", "error": err.Error()})
				return
			}
			s.write(ctx, map[string]any{"type": "queued", "mode": m.Mode})
		}
		return
	}

	if s.arena != nil {
		switch m.Type {
		case "fire":
			s.arena.Push(arena.Command{
				PlayerID: s.id, Kind: arena.CmdFire, AimX: m.AimX, AimY: m.AimY,
			})
		case "input":
			s.arena.Push(arena.Command{
				PlayerID: s.id, Kind: arena.CmdMove,
				Up: m.Up, Down: m.Down, Left: m.Left, Right: m.Right,
			})
		}
		return
	}

	switch m.Type {
	case "chat":
		ctx.Send(s.region, world.Chat{PlayerID: s.id, Text: m.Text})
	case "gather":
		ctx.Send(s.region, world.Gather{PlayerID: s.id})
	case "input":
		ctx.Send(s.region, world.Input{
			PlayerID: s.id,
			Up:       m.Up, Down: m.Down, Left: m.Left, Right: m.Right,
		})
	}
}

// onEnterArena: FENCING kontrolü, hub'dan ayrılma, arena moduna geçiş.
func (s *session) onEnterArena(ctx *actor.Context, m enterArenaMsg) {
	if m.token < s.token {
		m.reply <- errStaleToken
		return
	}
	s.token = m.token
	if s.region != nil {
		ctx.Send(s.region, world.Leave{PlayerID: s.id})
	}
	s.arena, s.team = m.a, m.team
	s.write(ctx, map[string]any{
		"type": "arena-enter", "arenaId": m.a.ID(), "mode": string(m.a.Mode()),
		"team": m.team, "w": arena.Width, "h": arena.Height, "tickRate": arena.TickRate,
	})
	m.reply <- nil
}

// onEnterHub: arenadan çıkış, hub bölgesine yeniden katılma.
func (s *session) onEnterHub(ctx *actor.Context, m enterHubMsg) {
	if m.token < s.token {
		m.reply <- errStaleToken
		return
	}
	s.token = m.token
	s.arena, s.team = nil, 0

	rid, shard, ref := s.router.SpawnRegion(world.Width/2, world.Height/2)
	s.region, s.regionID, s.shard = ref, rid, shard
	ctx.Send(ref, world.Join{
		PlayerID: s.id, Name: s.name, Session: ctx.Self(),
		X: world.Width / 2, Y: world.Height / 2,
	})
	s.write(ctx, map[string]any{"type": "hub-enter", "region": rid, "shard": shard})
	m.reply <- nil
}

// PostStop: bağlantı hangi yoldan koparsa kopsun güncel bölgeden ayrıl ve
// soketi kapat. Leave idempotent; handoff sırasında kopsa bile en fazla
// eski bölgeye zararsız bir Leave gider.
func (s *session) PostStop(ctx *actor.Context) {
	if s.arena != nil {
		// Arenadayken kopan bağlantı: oyuncu elenir (maç askıda kalmasın).
		s.arena.Push(arena.Command{PlayerID: s.id, Kind: arena.CmdLeave})
	}
	if s.region != nil {
		ctx.Send(s.region, world.Leave{PlayerID: s.id})
	}
	s.conn.Close()
}

// ---- handoff.SessionPort ve matchmaking.Assigner implementasyonları ----

func (g *Gateway) sessionRef(playerID string) *actor.Ref {
	g.sessMu.RLock()
	defer g.sessMu.RUnlock()
	return g.sessions[playerID]
}

// ask, oturuma emir gönderip cevabını bekler (framework'te ask yok;
// yanıt kanalı mesajın içinde taşınır).
func ask(ref *actor.Ref, send func(chan error), timeout time.Duration) error {
	reply := make(chan error, 1)
	send(reply)
	select {
	case err := <-reply:
		return err
	case <-ref.Stopped():
		return errors.New("gateway: session stopped")
	case <-time.After(timeout):
		return errors.New("gateway: session did not respond")
	}
}

// EnterArena, handoff.SessionPort.
func (g *Gateway) EnterArena(playerID string, a *arena.Arena, team int, token uint64) error {
	ref := g.sessionRef(playerID)
	if ref == nil {
		return fmt.Errorf("gateway: session %s not connected", playerID)
	}
	return ask(ref, func(reply chan error) {
		ref.Send(enterArenaMsg{a: a, team: team, token: token, reply: reply})
	}, 3*time.Second)
}

// EnterHub, handoff.SessionPort.
func (g *Gateway) EnterHub(playerID string, token uint64) error {
	ref := g.sessionRef(playerID)
	if ref == nil {
		return fmt.Errorf("gateway: session %s not connected", playerID)
	}
	return ask(ref, func(reply chan error) {
		ref.Send(enterHubMsg{token: token, reply: reply})
	}, 3*time.Second)
}

// Assign, matchmaking.Assigner: eşleşen oyuncuyu handoff ile arenaya al.
func (g *Gateway) Assign(playerID string, h *matchmaking.Handle, team int) error {
	if h == nil || h.Arena == nil {
		return errors.New("gateway: remote arena handles not supported yet")
	}
	return g.cfg.Handoff.ToArena(playerID, h.Arena, team)
}

// Release, matchmaking.Assigner telafisi / maç sonu: oyuncuyu hub'a döndür.
func (g *Gateway) Release(playerID string) {
	if err := g.cfg.Handoff.ToHub(playerID); err != nil {
		log.Printf("gateway: release %s: %v", playerID, err)
	}
}

func (s *session) write(ctx *actor.Context, v any) {
	s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := s.conn.WriteJSON(v); err != nil {
		ctx.Self().Stop() // yazamıyorsak oturum ölüdür
	}
}
