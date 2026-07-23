// Package arena, talep üzerine açılan geçici dövüş instance'larıdır.
//
// Hub'dan farkı BİLİNÇLİ bir profil farkıdır:
//
//	hub    → tutarlılık öncelikli, 20Hz, her komut bir AKTÖR MESAJI,
//	         kalıcı event log, shard/Raft ile sahiplik
//	arena  → gecikme öncelikli, 30Hz, komutlar LOCK-FREE RING BUFFER'a
//	         yazılır ve tick başına TOPLU boşaltılır; durum geçicidir,
//	         yalnız SONUÇ kalıcılaşır
//
// Arena bir aktör DEĞİLDİR: mesaj başına mailbox turu yerine, frame
// başına tek boşaltma yapan kendi tick döngüsü vardır (klasik oyun
// sunucusu deseni). Girdi kuyruğu Faz 0'da yazdığımız pkg/ringbuf'tır —
// üreticiler (oturum goroutine'leri) kilitsiz yazar, tek tüketici (tick
// döngüsü) okur.
package arena

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"shardlands/pkg/metrics"
	"shardlands/pkg/ringbuf"
)

const (
	Width  = 600.0
	Height = 400.0

	// TickRate hub'dan (20Hz) yüksektir: dövüş tepkiselliği için.
	TickRate = 30
	dt       = 1.0 / TickRate

	Speed            = 220.0 // px/s
	MaxHealth        = 100
	ProjectileSpeed  = 420.0 // px/s
	ProjectileRadius = 5.0
	PlayerRadius     = 14.0
	ProjectileDamage = 12
	FireCooldownTick = TickRate / 3 // ~333ms

	// MatchTicks, maç süresi (90 sn).
	MatchTicks = 90 * TickRate

	// inputCapacity: kuyruk dolarsa yeni komut DÜŞER — arena'da eskimiş
	// girdiyi beklemektense atmak doğrudur (gecikme öncelikli profil).
	inputCapacity = 1024
	// maxDrainPerTick: tek taşkın istemci frame'i uzatamasın.
	maxDrainPerTick = 256
)

// Mode, arena türü.
type Mode string

const (
	Mode1v1 Mode = "1v1"
	Mode2v2 Mode = "2v2"
)

// TeamSize, moda göre takım başına oyuncu sayısı.
func (m Mode) TeamSize() int {
	if m == Mode2v2 {
		return 2
	}
	return 1
}

// CmdKind, girdi türü.
type CmdKind uint8

const (
	CmdMove CmdKind = iota
	CmdFire
	CmdLeave
)

// Command, oturumdan arenaya gelen girdi. Ring buffer'a DEĞER olarak
// yazılır (pointer yok → allocation yok, cache dostu).
type Command struct {
	PlayerID              string
	Kind                  CmdKind
	Up, Down, Left, Right bool
	AimX, AimY            float64 // CmdFire: nişan yönü
}

// Sink, bir oyuncuya snapshot teslim eden alıcıdır. İki gerçekleşme:
// yerel yolda oturum aktörüne mesaj gönderen adaptör, uzak yolda
// (arena Pod'u) gRPC akışına yazan adaptör. Arena böylece aktör
// framework'üne bağımlı değildir — Pod binary'si onu taşımaz.
//
// Deliver BLOKLAMAMALIDIR: tick döngüsünü yavaşlatır. Yavaş alıcı
// kareyi düşürmelidir (arena profili: eskimiş kareyi beklemek yerine at).
type Sink interface {
	Deliver(Snapshot)
}

// PlayerSpec, arenaya girecek oyuncunun tanımı.
type PlayerSpec struct {
	ID   string
	Name string
	Team int  // 0 veya 1
	Sink Sink // snapshot alıcısı (nil olabilir; sonradan SetSink ile de bağlanır)
}

// PlayerState, istemciye görünen oyuncu.
type PlayerState struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Team   int     `json:"team"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Health int     `json:"health"`
	Alive  bool    `json:"alive"`
}

// ProjectileState, uçan mermi.
type ProjectileState struct {
	ID   uint64  `json:"id"`
	Team int     `json:"team"`
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
}

// Snapshot, tick sonrası arena durumu.
type Snapshot struct {
	ArenaID     string            `json:"arenaId"`
	Tick        uint64            `json:"tick"`
	RemainingMs int64             `json:"remainingMs"`
	Players     []PlayerState     `json:"players"`
	Projectiles []ProjectileState `json:"projectiles"`
	Over        bool              `json:"over"`
	WinnerTeam  int               `json:"winnerTeam"` // -1 = beraberlik
}

// Result, maçın kalıcılaşacak tek çıktısı.
type Result struct {
	ArenaID    string
	Mode       Mode
	WinnerTeam int // -1 beraberlik
	Ticks      uint64
	Damage     map[string]int
	Survivors  []string
}

type player struct {
	PlayerSpec
	x, y         float64
	health       int
	in           Command
	fireCooldown int
	damageDealt  int
}

func (p *player) alive() bool { return p.health > 0 }

type projectile struct {
	id     uint64
	team   int
	owner  string
	x, y   float64
	vx, vy float64
}

// Arena, tek bir maç instance'ı.
type Arena struct {
	id   string
	mode Mode

	// inputs: oturumların KİLİTSİZ yazdığı komut kuyruğu; yalnız tick
	// döngüsü okur (MPSC).
	inputs *ringbuf.MPSC[Command]

	// Aşağıdakilere YALNIZ tick döngüsü dokunur (kilitsiz).
	players     []*player
	byID        map[string]*player
	projectiles []*projectile
	nextProjID  uint64
	tick        uint64

	// Dışarıdan okunanlar kilitli.
	mu       sync.RWMutex
	snapshot Snapshot
	result   *Result
	sinks    map[string]Sink // playerID → snapshot alıcısı

	dropped atomic.Int64

	onEnd    func(Result)
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// Options, arena kurulumu.
type Options struct {
	// OnEnd, maç bittiğinde bir kez çağrılır (oyuncuları hub'a döndürmek
	// için matchmaking/handoff kullanır).
	OnEnd func(Result)
}

// New, oyuncularla bir arena kurar (henüz tick'lemez).
func New(id string, mode Mode, specs []PlayerSpec, opts Options) *Arena {
	a := &Arena{
		id:     id,
		mode:   mode,
		inputs: ringbuf.New[Command](inputCapacity),
		byID:   map[string]*player{},
		sinks:  map[string]Sink{},
		onEnd:  opts.OnEnd,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	// Takımlara göre karşılıklı diz.
	counts := map[int]int{}
	for _, s := range specs {
		p := &player{PlayerSpec: s, health: MaxHealth}
		idx := counts[s.Team]
		counts[s.Team]++
		p.x, p.y = spawnPos(s.Team, idx)
		a.players = append(a.players, p)
		a.byID[s.ID] = p
		if s.Sink != nil {
			a.sinks[s.ID] = s.Sink
		}
	}
	sort.Slice(a.players, func(i, j int) bool { return a.players[i].ID < a.players[j].ID })
	a.publish()
	return a
}

// spawnPos, takımları karşılıklı kenarlara, takım içindekileri dikeyde
// ayırarak diz.
func spawnPos(team, idx int) (float64, float64) {
	x := 80.0
	if team == 1 {
		x = Width - 80
	}
	y := Height / 2
	if idx > 0 {
		y += 90
	}
	return x, y
}

// ID / Mode erişimcileri.
func (a *Arena) ID() string { return a.id }
func (a *Arena) Mode() Mode { return a.mode }

// Push, komutu KİLİTSİZ kuyruğa koyar. Herhangi bir goroutine'den
// çağrılabilir. Kuyruk doluysa false döner (komut düşer).
func (a *Arena) Push(c Command) bool {
	if a.inputs.TryPush(c) {
		return true
	}
	a.dropped.Add(1)
	return false
}

// Dropped, kuyruk taşması yüzünden düşen komut sayısı (gözlem).
func (a *Arena) Dropped() int64 { return a.dropped.Load() }

// Snapshot, son yayınlanan durumu döner.
func (a *Arena) Snapshot() Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.snapshot
}

// Result, maç bittiyse sonucu döner.
func (a *Arena) Result() (Result, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.result == nil {
		return Result{}, false
	}
	return *a.result, true
}

// Run, gerçek zamanlı tick döngüsünü başlatır (üretim yolu).
func (a *Arena) Run() {
	go func() {
		defer close(a.done)
		t := time.NewTicker(time.Second / TickRate)
		defer t.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-t.C:
				// Ölçüm DÖNGÜDE, Tick()'in içinde değil: benchmark'ı
				// (39.8 ns) kirletmemek için — bkz. pkg/metrics.
				basla := time.Now()
				bitti := a.Tick()
				metrics.ArenaTickDuration.Observe(time.Since(basla).Seconds())
				if bitti {
					return // maç bitti
				}
			}
		}
	}()
}

// Stop, döngüyü durdurur.
func (a *Arena) Stop() {
	a.stopOnce.Do(func() { close(a.stop) })
}

// Wait, döngünün bitmesini bekler (Run çağrıldıysa).
func (a *Arena) Wait() { <-a.done }

// Tick, tek bir simülasyon adımıdır ve maç bittiyse true döner.
// Testlerde elle çağrılır (deterministik), üretimde Run çağırır.
func (a *Arena) Tick() bool {
	if a.finished() {
		return true
	}
	a.drainInputs()
	a.step()
	over, winner := a.checkEnd()
	a.tick++
	a.publishWith(over, winner)
	if over {
		a.finish(winner)
		return true
	}
	return false
}

func (a *Arena) finished() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.result != nil
}

// drainInputs, kuyruğu tick başına TOPLU boşaltır: her komut için ayrı
// senkronizasyon turu yok (ring buffer'ın kazancı burada).
func (a *Arena) drainInputs() {
	for i := 0; i < maxDrainPerTick; i++ {
		c, ok := a.inputs.TryPop()
		if !ok {
			return
		}
		p := a.byID[c.PlayerID]
		if p == nil {
			continue
		}
		switch c.Kind {
		case CmdMove:
			p.in = c
		case CmdFire:
			a.fire(p, c)
		case CmdLeave:
			p.health = 0 // ayrılan oyuncu elenir
		}
	}
}

func (a *Arena) fire(p *player, c Command) {
	if !p.alive() || p.fireCooldown > 0 {
		return
	}
	dx, dy := c.AimX, c.AimY
	n := math.Hypot(dx, dy)
	if n == 0 {
		return
	}
	dx, dy = dx/n, dy/n
	p.fireCooldown = FireCooldownTick
	a.nextProjID++
	a.projectiles = append(a.projectiles, &projectile{
		id: a.nextProjID, team: p.Team, owner: p.ID,
		x: p.x, y: p.y, vx: dx * ProjectileSpeed, vy: dy * ProjectileSpeed,
	})
}

func (a *Arena) step() {
	// Oyuncular.
	for _, p := range a.players {
		if p.fireCooldown > 0 {
			p.fireCooldown--
		}
		if !p.alive() {
			continue
		}
		var vx, vy float64
		if p.in.Left {
			vx -= Speed
		}
		if p.in.Right {
			vx += Speed
		}
		if p.in.Up {
			vy -= Speed
		}
		if p.in.Down {
			vy += Speed
		}
		p.x = clamp(p.x+vx*dt, PlayerRadius, Width-PlayerRadius)
		p.y = clamp(p.y+vy*dt, PlayerRadius, Height-PlayerRadius)
	}

	// Mermiler: hareket + çarpışma.
	alive := a.projectiles[:0]
	for _, pr := range a.projectiles {
		pr.x += pr.vx * dt
		pr.y += pr.vy * dt
		if pr.x < 0 || pr.x > Width || pr.y < 0 || pr.y > Height {
			continue // sahayı terk etti
		}
		hit := false
		for _, p := range a.players {
			if !p.alive() || p.Team == pr.team {
				continue
			}
			if math.Hypot(p.x-pr.x, p.y-pr.y) <= PlayerRadius+ProjectileRadius {
				p.health -= ProjectileDamage
				if p.health < 0 {
					p.health = 0
				}
				if owner := a.byID[pr.owner]; owner != nil {
					owner.damageDealt += ProjectileDamage
				}
				hit = true
				break
			}
		}
		if !hit {
			alive = append(alive, pr)
		}
	}
	a.projectiles = alive
}

// checkEnd: bir takım tamamen elendiyse veya süre dolduysa maç biter.
func (a *Arena) checkEnd() (over bool, winner int) {
	aliveByTeam := map[int]int{}
	healthByTeam := map[int]int{}
	for _, p := range a.players {
		healthByTeam[p.Team] += p.health
		if p.alive() {
			aliveByTeam[p.Team]++
		}
	}
	t0, t1 := aliveByTeam[0], aliveByTeam[1]
	switch {
	case t0 == 0 && t1 == 0:
		return true, -1
	case t0 == 0:
		return true, 1
	case t1 == 0:
		return true, 0
	}
	if a.tick+1 >= MatchTicks {
		// Süre doldu: toplam canı fazla olan kazanır.
		switch {
		case healthByTeam[0] > healthByTeam[1]:
			return true, 0
		case healthByTeam[1] > healthByTeam[0]:
			return true, 1
		default:
			return true, -1
		}
	}
	return false, -1
}

func (a *Arena) finish(winner int) {
	res := Result{
		ArenaID: a.id, Mode: a.mode, WinnerTeam: winner,
		Ticks: a.tick, Damage: map[string]int{},
	}
	for _, p := range a.players {
		res.Damage[p.ID] = p.damageDealt
		if p.alive() {
			res.Survivors = append(res.Survivors, p.ID)
		}
	}
	sort.Strings(res.Survivors)

	a.mu.Lock()
	a.result = &res
	a.mu.Unlock()

	a.Stop()
	if a.onEnd != nil {
		a.onEnd(res)
	}
}

func (a *Arena) publish() { a.publishWith(false, -1) }

// publishWith, snapshot'ı hazırlar ve oturumlara yayınlar.
func (a *Arena) publishWith(over bool, winner int) {
	snap := Snapshot{
		ArenaID: a.id, Tick: a.tick, Over: over, WinnerTeam: winner,
		RemainingMs: remainingMs(a.tick),
		Players:     make([]PlayerState, 0, len(a.players)),
		Projectiles: make([]ProjectileState, 0, len(a.projectiles)),
	}
	for _, p := range a.players {
		snap.Players = append(snap.Players, PlayerState{
			ID: p.ID, Name: p.Name, Team: p.Team,
			X: p.x, Y: p.y, Health: p.health, Alive: p.alive(),
		})
	}
	for _, pr := range a.projectiles {
		snap.Projectiles = append(snap.Projectiles, ProjectileState{
			ID: pr.id, Team: pr.team, X: pr.x, Y: pr.y,
		})
	}

	// Alıcıları kilit ALTINDA kopyala, teslimi kilit DIŞINDA yap:
	// Deliver'ın yavaşlığı tick döngüsünü ve okuyucuları kilitlemesin.
	a.mu.Lock()
	a.snapshot = snap
	sinks := make([]Sink, 0, len(a.sinks))
	for _, s := range a.sinks {
		sinks = append(sinks, s)
	}
	a.mu.Unlock()

	for _, s := range sinks {
		s.Deliver(snap)
	}
}

// SetSink, oyuncunun snapshot alıcısını (yeniden) bağlar. Uzak arenada
// oyuncular arena kurulduktan SONRA bağlandığı için gereklidir.
func (a *Arena) SetSink(playerID string, s Sink) {
	a.mu.Lock()
	if s == nil {
		delete(a.sinks, playerID)
	} else {
		a.sinks[playerID] = s
	}
	a.mu.Unlock()
}

// HasPlayer, oyuncunun bu arenaya ait olup olmadığı.
func (a *Arena) HasPlayer(playerID string) bool {
	_, ok := a.byID[playerID]
	return ok
}

func remainingMs(tick uint64) int64 {
	if tick >= MatchTicks {
		return 0
	}
	return int64(float64(MatchTicks-tick) / TickRate * 1000)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
