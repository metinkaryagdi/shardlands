package world

import (
	"encoding/json"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"shardlands/pkg/actor"
	"shardlands/pkg/es"
	"shardlands/pkg/metrics"
	"shardlands/services/inventory"
)

// region, tek bir grid bölgesinin aktörüdür: kendi oyuncularını,
// node'larını ve tick'ini yönetir. Durumuna yalnız kendi goroutine'i
// dokunur (kilitsiz). Faz 1'deki tek "hub" aktörünün bölgeye
// ölçeklenmiş hâli.
type region struct {
	id, shard              string
	minX, minY, maxX, maxY float64
	tick                   uint64
	players                map[string]*entity
	nodes                  []*node
	events                 *es.Store
	router                 *Router
}

func regionProps(id string, col, row int, shard string, events *es.Store, router *Router) actor.Props {
	minX, minY := float64(col)*RegionW, float64(row)*RegionH
	return actor.Props{
		Name: id,
		Producer: func() actor.Actor {
			r := &region{
				id: id, shard: shard,
				minX: minX, minY: minY, maxX: minX + RegionW, maxY: minY + RegionH,
				players: map[string]*entity{}, events: events, router: router,
			}
			for _, n := range nodeLayout {
				if RegionAt(n.X, n.Y) == id {
					r.nodes = append(r.nodes, &node{NodeState: n})
				}
			}
			return r
		},
	}
}

type entity struct {
	id, name    string
	x, y        float64
	in          Input
	session     *actor.Ref
	bubble      string
	bubbleUntil uint64
}

type node struct {
	NodeState
	respawnAt uint64
}

func (n *node) available() bool { return n.respawnAt == 0 }

func (r *region) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case Join:
		r.players[m.PlayerID] = &entity{
			id: m.PlayerID, name: m.Name,
			x: clamp(m.X, 0, Width), y: clamp(m.Y, 0, Height),
			in: m.In, session: m.Session,
		}
	case Leave:
		delete(r.players, m.PlayerID)
	case Input:
		if e, ok := r.players[m.PlayerID]; ok {
			e.in = m
		}
	case Chat:
		r.handleChat(m)
	case Gather:
		r.handleGather(m)
	case Tick:
		// Tick süresi hub'ın sağlığının EN DOĞRUDAN göstergesi: 20Hz'de
		// bütçe 50ms. Bütçeye yaklaşan bir tick, oyuncular donuklaşmayı
		// hissetmeden önce panoda görünür.
		basla := time.Now()
		r.tickStep(ctx)
		metrics.WorldTickDuration.Observe(time.Since(basla).Seconds())
	}
}

// frozen: bu bölgeyi barındıran shard hizmet veremiyorsa (Raft grubunda
// çoğunluk yok) bölge DONAR — simülasyon ilerlemez, komut kabul edilmez.
// CAP'in C tarafı: tutarlılık için kullanılabilirlikten vazgeçiyoruz.
func (r *region) frozen() bool { return !r.router.ShardUp(r.shard) }

func (r *region) handleChat(m Chat) {
	if r.frozen() {
		return
	}
	e, ok := r.players[m.PlayerID]
	if !ok {
		return
	}
	text := strings.TrimSpace(m.Text)
	if text == "" || len([]rune(text)) > maxChatLen {
		return
	}
	e.bubble = text
	e.bubbleUntil = r.tick + bubbleTicks
	if r.events == nil {
		return
	}
	data, _ := json.Marshal(ChatSaid{PlayerID: e.id, Name: e.name, Text: text})
	if _, err := r.events.Append(ChatStream, es.AnyVersion, es.EventData{Type: EventChatSaid, Data: data}); err != nil {
		log.Printf("region %s: chat append: %v", r.id, err)
	}
}

func (r *region) handleGather(m Gather) {
	if r.frozen() {
		return
	}
	e, ok := r.players[m.PlayerID]
	if !ok {
		return
	}
	var best *node
	bestDist := GatherRadius
	for _, n := range r.nodes {
		if !n.available() {
			continue
		}
		if d := math.Hypot(n.X-e.x, n.Y-e.y); d <= bestDist {
			best, bestDist = n, d
		}
	}
	if best == nil {
		return
	}
	best.respawnAt = r.tick + RespawnTicks
	if r.events == nil {
		return
	}
	data, _ := json.Marshal(inventory.Gathered{
		PlayerID: e.id, Name: e.name, NodeID: best.ID, Kind: best.Kind, Amount: 1,
	})
	if _, err := r.events.Append(inventory.Stream(e.id), es.AnyVersion,
		es.EventData{Type: inventory.EventGathered, Data: data}); err != nil {
		log.Printf("region %s: gather append: %v", r.id, err)
	}
}

// tickStep: fiziği ilerlet, sınır geçenleri HANDOFF et, kalanlara
// snapshot yayınla.
func (r *region) tickStep(ctx *actor.Context) {
	if r.frozen() {
		return // shard kullanılamaz: ilerleme yok, snapshot yok
	}
	const dt = 1.0 / TickRate
	for _, n := range r.nodes {
		if n.respawnAt != 0 && r.tick >= n.respawnAt {
			n.respawnAt = 0
		}
	}

	var handed []string
	for _, e := range r.players {
		if e.bubbleUntil != 0 && r.tick >= e.bubbleUntil {
			e.bubble, e.bubbleUntil = "", 0
		}
		var vx, vy float64
		if e.in.Left {
			vx -= Speed
		}
		if e.in.Right {
			vx += Speed
		}
		if e.in.Up {
			vy -= Speed
		}
		if e.in.Down {
			vy += Speed
		}
		nx := clamp(e.x+vx*dt, 0, Width)
		ny := clamp(e.y+vy*dt, 0, Height)

		if dst := RegionAt(nx, ny); dst != r.id {
			if ref := r.router.Ref(dst); ref != nil && r.router.RegionShardUp(dst) {
				// HANDOFF: hedef bölgeye devret, oturuma yeni bölgeyi bildir.
				ctx.Send(ref, Join{PlayerID: e.id, Name: e.name, Session: e.session, X: nx, Y: ny, In: e.in})
				ctx.Send(e.session, AssignedRegion{RegionID: dst, Shard: r.router.ShardOf(dst), Ref: ref})
				handed = append(handed, e.id)
				continue
			}
			// Hedef shard down/yok: sınırı geçme, bu bölgede kal (CAP:
			// izole shard'ın bölgesi kullanılamaz — oyuncu sınırda durur).
			nx = clamp(nx, r.minX, r.maxX-1)
			ny = clamp(ny, r.minY, r.maxY-1)
		}
		e.x, e.y = nx, ny
	}
	for _, id := range handed {
		delete(r.players, id)
	}

	r.tick++
	snap := r.snapshot()
	for _, e := range r.players {
		ctx.Send(e.session, snap)
	}
}

func (r *region) snapshot() Snapshot {
	ps := make([]PlayerState, 0, len(r.players))
	for _, e := range r.players {
		ps = append(ps, PlayerState{ID: e.id, Name: e.name, X: e.x, Y: e.y, Bubble: e.bubble})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].ID < ps[j].ID })
	ns := make([]NodeState, len(r.nodes))
	for i, n := range r.nodes {
		ns[i] = n.NodeState
		ns[i].Available = n.available()
	}
	return Snapshot{Tick: r.tick, RegionID: r.id, Shard: r.shard, Players: ps, Nodes: ns}
}
