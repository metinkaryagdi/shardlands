package matchmaking

import (
	"context"
	"fmt"
	"sync"

	"shardlands/services/arena"
)

// ArenaSpec, sağlanacak arena instance'ının tanımıdır. Provisioner'ın
// nerede çalıştığından (bu süreç, ayrı pod, ayrı küme) bağımsızdır —
// Faz 5'in K8s Operator'ü aynı spec'i CRD'ye çevirecek.
type ArenaSpec struct {
	ID      string
	Mode    arena.Mode
	Players []arena.PlayerSpec
	// OnEnd, maç bitince çağrılır (oyuncuları hub'a döndürmek için).
	OnEnd func(arena.Result)
}

// Handle, sağlanmış bir arenaya tutamaç. Local sağlayıcıda doğrudan
// instance; uzak sağlayıcıda yalnız kimlik ve adres dolu olur.
type Handle struct {
	ID       string
	Mode     string       // "1v1" | "2v2"
	Arena    *arena.Arena // yalnız yerel sağlayıcıda
	Endpoint string       // uzak sağlayıcıda (arena Pod'unun adresi)
}

// Provisioner, arena instance'ı yaratıp yok eden bileşendir. Saga bu
// arayüzü çağırır; telafi (Destroy) da buradan gelir.
type Provisioner interface {
	Provision(ctx context.Context, spec ArenaSpec) (*Handle, error)
	Destroy(ctx context.Context, arenaID string) error
}

// LocalProvisioner, arenayı BU süreçte çalıştırır (tek node kurulum ve
// testler). Kubernetes sağlayıcısı Faz 5'in operator adımında gelir;
// saga tarafı hiç değişmeyecek — arayüzün amacı bu.
type LocalProvisioner struct {
	mu     sync.Mutex
	arenas map[string]*arena.Arena
}

func NewLocalProvisioner() *LocalProvisioner {
	return &LocalProvisioner{arenas: map[string]*arena.Arena{}}
}

func (p *LocalProvisioner) Provision(_ context.Context, spec ArenaSpec) (*Handle, error) {
	if spec.ID == "" {
		return nil, fmt.Errorf("matchmaking: arena id is required")
	}
	p.mu.Lock()
	if _, exists := p.arenas[spec.ID]; exists {
		p.mu.Unlock()
		return nil, fmt.Errorf("matchmaking: arena %s already exists", spec.ID)
	}
	a := arena.New(spec.ID, spec.Mode, spec.Players, arena.Options{OnEnd: spec.OnEnd})
	p.arenas[spec.ID] = a
	p.mu.Unlock()

	a.Run()
	return &Handle{ID: spec.ID, Mode: string(spec.Mode), Arena: a}, nil
}

// Destroy, arenayı durdurur. İdempotenttir: olmayan arenayı yok etmek
// hata değildir (telafi yolları tekrar çalışabilir).
func (p *LocalProvisioner) Destroy(_ context.Context, arenaID string) error {
	p.mu.Lock()
	a := p.arenas[arenaID]
	delete(p.arenas, arenaID)
	p.mu.Unlock()
	if a != nil {
		a.Stop()
	}
	return nil
}

// Count, canlı arena sayısı (gözlem/test).
func (p *LocalProvisioner) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.arenas)
}

// Get, arenayı döner (test/gözlem).
func (p *LocalProvisioner) Get(arenaID string) *arena.Arena {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.arenas[arenaID]
}
