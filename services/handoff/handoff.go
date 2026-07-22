// Package handoff, oyuncunun hub ile arena arasındaki transferini
// koordine eder.
//
// Neden ayrı bir bileşen? Transfer, iki bağımsız dünyanın (kalıcı hub
// bölgesi ve geçici arena instance'ı) arasında oyuncunun TEK sahipliğini
// taşımak demektir. İki tehlike var:
//
//  1. ÇİFTE TRANSFER: iki maç aynı oyuncuyu kapabilir; ya da maç bitişi
//     (hub'a dönüş) ile yeni bir atama yarışabilir. Çözüm: oyuncu başına
//     DAĞITIK KİLİT (pkg/dlock) — "tek sahip" bir anlaşma problemidir.
//  2. GECİKMİŞ TRANSFER: kilidi almış ama duraklamış bir koordinatörün
//     emri, kilit el değiştirdikten SONRA oturuma ulaşabilir. Kilit tek
//     başına bunu çözmez; çözüm FENCING TOKEN — token'ı korunan kaynak
//     (oturum) doğrular ve gördüğünden küçük token'lı emri REDDEDER.
//
// Her transfer event store'a yazılır: handoff-<playerID> stream'i
// oyuncunun nereden nereye, hangi token'la geçtiğinin denetim izidir.
package handoff

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"shardlands/pkg/dlock"
	"shardlands/pkg/es"
	"shardlands/services/arena"
)

// Denetim izi event tipleri.
const (
	EventLeftHub       = "PlayerLeftHub"
	EventEnteredArena  = "PlayerEnteredArena"
	EventLeftArena     = "PlayerLeftArena"
	EventEnteredHub    = "PlayerEnteredHub"
	EventHandoffFailed = "HandoffFailed"
)

// Stream, oyuncunun handoff denetim izi stream'i.
func Stream(playerID string) string { return "handoff-" + playerID }

// Record, denetim event'lerinin ortak veri şekli.
type Record struct {
	PlayerID string `json:"playerId"`
	ArenaID  string `json:"arenaId,omitempty"`
	Team     int    `json:"team,omitempty"`
	Token    uint64 `json:"token,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// ErrBusy: oyuncu için başka bir transfer sürüyor (kilit alınamadı).
var ErrBusy = errors.New("handoff: player transfer already in progress")

// SessionPort, oturuma transfer emri veren arayüzdür (gateway uygular).
// token, fencing için taşınır: oturum gördüğünden küçük token'ı
// reddetmelidir.
type SessionPort interface {
	EnterArena(playerID string, a *arena.Arena, team int, token uint64) error
	EnterHub(playerID string, token uint64) error
}

// Coordinator, transferleri kilit + denetim izi ile yürütür.
type Coordinator struct {
	locks *dlock.Manager
	store *es.Store
	port  SessionPort
	node  string        // düğüm kimliği (Faz 6'da pod adı)
	ttl   time.Duration // kilit lease süresi
	seq   atomic.Uint64 // transfer başına benzersiz sahip üretmek için
}

// New, koordinatörü kurar. locks nil ise kilit atlanır (tek süreçli
// testler); store nil ise denetim izi yazılmaz.
func New(locks *dlock.Manager, store *es.Store, port SessionPort, node string) *Coordinator {
	if node == "" {
		node = "gateway-0"
	}
	return &Coordinator{locks: locks, store: store, port: port, node: node, ttl: 10 * time.Second}
}

func lockKey(playerID string) string { return "player/" + playerID }

// acquire, oyuncu kilidini alır ve fencing token'ı döner.
//
// Sahip kimliği HER TRANSFER İÇİN BENZERSİZDİR ("<node>/<n>"). Neden?
// Kilitler aynı sahibin yeniden almasını idempotent kabul eder (doğru
// kilit semantiği); oysa biz iki AYRI transferi — aynı süreçten gelseler
// bile — birbirinden dışlamak istiyoruz. Benzersiz sahip, ikinci
// transferi "başkası tutuyor" durumuna düşürür.
func (c *Coordinator) acquire(playerID string) (uint64, func(), error) {
	if c.locks == nil {
		// Kilitsiz mod (tek süreç testleri): token 0; oturum 0'ı kabul eder.
		return 0, func() {}, nil
	}
	owner := fmt.Sprintf("%s/%d", c.node, c.seq.Add(1))
	token, ok, err := c.locks.Acquire(lockKey(playerID), owner, c.ttl)
	if err != nil {
		return 0, nil, err
	}
	if !ok {
		return 0, nil, ErrBusy
	}
	return token, func() { c.locks.Release(lockKey(playerID), owner) }, nil
}

// ToArena, oyuncuyu hub'dan arenaya taşır.
func (c *Coordinator) ToArena(playerID string, a *arena.Arena, team int) error {
	token, release, err := c.acquire(playerID)
	if err != nil {
		c.audit(playerID, EventHandoffFailed, Record{PlayerID: playerID, Reason: err.Error()})
		return err
	}
	defer release()

	if err := c.port.EnterArena(playerID, a, team, token); err != nil {
		// Oturum emri reddetti (bayat token veya kapalı oturum):
		// hiçbir şey taşınmadı, yalnız kaydet.
		c.audit(playerID, EventHandoffFailed, Record{
			PlayerID: playerID, ArenaID: a.ID(), Token: token, Reason: err.Error(),
		})
		return err
	}
	rec := Record{PlayerID: playerID, ArenaID: a.ID(), Team: team, Token: token}
	c.audit(playerID, EventLeftHub, rec)
	c.audit(playerID, EventEnteredArena, rec)
	return nil
}

// ToHub, oyuncuyu arenadan hub'a geri taşır (maç bitişi veya iptal).
func (c *Coordinator) ToHub(playerID string) error {
	token, release, err := c.acquire(playerID)
	if err != nil {
		c.audit(playerID, EventHandoffFailed, Record{PlayerID: playerID, Reason: err.Error()})
		return err
	}
	defer release()

	if err := c.port.EnterHub(playerID, token); err != nil {
		c.audit(playerID, EventHandoffFailed, Record{
			PlayerID: playerID, Token: token, Reason: err.Error(),
		})
		return err
	}
	rec := Record{PlayerID: playerID, Token: token}
	c.audit(playerID, EventLeftArena, rec)
	c.audit(playerID, EventEnteredHub, rec)
	return nil
}

func (c *Coordinator) audit(playerID, typ string, rec Record) {
	if c.store == nil {
		return
	}
	data, _ := json.Marshal(rec)
	if _, err := c.store.Append(Stream(playerID), es.AnyVersion,
		es.EventData{Type: typ, Data: data}); err != nil {
		log.Printf("handoff: audit %s: %v", typ, err)
	}
}
