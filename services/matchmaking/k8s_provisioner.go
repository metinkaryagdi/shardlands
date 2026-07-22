package matchmaking

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	arenav1 "shardlands/operator/api/v1alpha1"
)

// K8sProvisioner, arenayı Kubernetes'te ARENA CRD'si olarak sağlar.
//
// Dikkat: saga tarafı (services/matchmaking/saga.go) BU DOSYADAN
// HABERSİZDİR — Provisioner arayüzü sayesinde "arenayı bu süreçte mi
// yoksa kümede mi açtık" sorusu saga'nın problemi değildir. Arayüzü
// Faz 5'in başında koymanın kazancı tam olarak buydu.
//
// Deklaratif sağlama: burada Pod yaratmıyoruz, yalnız "şöyle bir arena
// istiyorum" kaydı bırakıyoruz. Pod'u operator'ün reconcile döngüsü
// yaratır ve yaşam döngüsünü (TTL, temizlik) o yönetir.
type K8sProvisioner struct {
	Client    client.Client
	Namespace string
	Image     string
	// ReadyTimeout, arenanın Running olup endpoint yayınlamasını
	// bekleme süresi (0 ise 30sn).
	ReadyTimeout time.Duration
	// Poll, durum yoklama aralığı (0 ise 250ms).
	Poll time.Duration
}

func (p *K8sProvisioner) namespace() string {
	if p.Namespace == "" {
		return "default"
	}
	return p.Namespace
}

func (p *K8sProvisioner) readyTimeout() time.Duration {
	if p.ReadyTimeout <= 0 {
		return 30 * time.Second
	}
	return p.ReadyTimeout
}

func (p *K8sProvisioner) poll() time.Duration {
	if p.Poll <= 0 {
		return 250 * time.Millisecond
	}
	return p.Poll
}

// Provision, Arena kaydını oluşturur ve hazır olmasını bekler.
func (p *K8sProvisioner) Provision(ctx context.Context, spec ArenaSpec) (*Handle, error) {
	if spec.ID == "" {
		return nil, fmt.Errorf("matchmaking: arena id is required")
	}
	players := make([]arenav1.ArenaPlayer, len(spec.Players))
	for i, s := range spec.Players {
		players[i] = arenav1.ArenaPlayer{ID: s.ID, Name: s.Name, Team: s.Team}
	}
	cr := &arenav1.Arena{
		ObjectMeta: metav1.ObjectMeta{Name: spec.ID, Namespace: p.namespace()},
		Spec: arenav1.ArenaSpec{
			Mode:    string(spec.Mode),
			Players: players,
			Image:   p.Image,
		},
	}
	if err := p.Client.Create(ctx, cr); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		// İdempotent: aynı maç için tekrar çağrı (saga yeniden denemesi).
	}

	// Operator'ün Pod'u ayağa kaldırıp endpoint yayınlamasını bekle.
	deadline := time.Now().Add(p.readyTimeout())
	for time.Now().Before(deadline) {
		var got arenav1.Arena
		if err := p.Client.Get(ctx, client.ObjectKey{Namespace: p.namespace(), Name: spec.ID}, &got); err != nil {
			return nil, err
		}
		switch got.Status.Phase {
		case arenav1.PhaseRunning:
			if got.Status.Endpoint != "" {
				return &Handle{ID: spec.ID, Endpoint: got.Status.Endpoint}, nil
			}
		case arenav1.PhaseFailed:
			// Telafi saga'da: kaydı bırakmayalım.
			_ = p.Destroy(ctx, spec.ID)
			return nil, fmt.Errorf("matchmaking: arena %s failed: %s", spec.ID, got.Status.Message)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(p.poll()):
		}
	}
	_ = p.Destroy(ctx, spec.ID) // hazır olmadı: sızıntı bırakma
	return nil, fmt.Errorf("matchmaking: arena %s not ready in time", spec.ID)
}

// Destroy, Arena kaydını siler; Pod'u operator'ün finalizer'ı temizler.
// İdempotenttir (olmayan kaydı silmek hata değildir).
func (p *K8sProvisioner) Destroy(ctx context.Context, arenaID string) error {
	cr := &arenav1.Arena{ObjectMeta: metav1.ObjectMeta{Name: arenaID, Namespace: p.namespace()}}
	if err := p.Client.Delete(ctx, cr); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
