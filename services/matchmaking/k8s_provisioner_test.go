package matchmaking

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	arenav1 "shardlands/operator/api/v1alpha1"
	"shardlands/services/arena"
)

func k8sFixture(t *testing.T) (*K8sProvisioner, client.Client) {
	t.Helper()
	s := runtime.NewScheme()
	if err := arenav1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&arenav1.Arena{}).Build()
	return &K8sProvisioner{
		Client: c, Namespace: "default", Image: "test:dev",
		ReadyTimeout: 3 * time.Second, Poll: 20 * time.Millisecond,
	}, c
}

func sampleSpec() ArenaSpec {
	return ArenaSpec{
		ID:   "arena-m1",
		Mode: arena.Mode1v1,
		Players: []arena.PlayerSpec{
			{ID: "p1", Name: "bir", Team: 0},
			{ID: "p2", Name: "iki", Team: 1},
		},
	}
}

// markRunning, operator'ü taklit eder: kaydı Running yapıp endpoint verir.
func markRunning(t *testing.T, c client.Client, name, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var a arenav1.Arena
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, &a); err == nil {
			a.Status.Phase = arenav1.PhaseRunning
			a.Status.Endpoint = endpoint
			if err := c.Status().Update(context.Background(), &a); err != nil {
				t.Errorf("status update: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("arena CR never appeared")
}

// Provision, CRD kaydı bırakır (Pod'u operator yaratır) ve endpoint
// yayınlanınca handle döner.
func TestK8sProvisionCreatesCRAndWaitsForEndpoint(t *testing.T) {
	p, c := k8sFixture(t)
	go markRunning(t, c, "arena-m1", "10.0.0.5:7777")

	h, err := p.Provision(context.Background(), sampleSpec())
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if h.Endpoint != "10.0.0.5:7777" || h.ID != "arena-m1" {
		t.Fatalf("handle = %+v", h)
	}
	if h.Arena != nil {
		t.Fatal("k8s handle must not carry an in-process arena")
	}

	var cr arenav1.Arena
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "arena-m1"}, &cr); err != nil {
		t.Fatal(err)
	}
	if cr.Spec.Mode != "1v1" || len(cr.Spec.Players) != 2 || cr.Spec.Image != "test:dev" {
		t.Fatalf("spec = %+v", cr.Spec)
	}
	if cr.Spec.Players[1].Team != 1 {
		t.Fatalf("teams not carried: %+v", cr.Spec.Players)
	}
}

// Arena Failed olursa hata döner ve kayıt SİLİNİR (sızıntı yok) —
// saga'nın telafisi bunun üstüne çalışır.
func TestK8sProvisionFailureCleansUp(t *testing.T) {
	p, c := k8sFixture(t)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			var a arenav1.Arena
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "arena-m1"}, &a); err == nil {
				a.Status.Phase = arenav1.PhaseFailed
				a.Status.Message = "image pull error"
				c.Status().Update(context.Background(), &a)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	if _, err := p.Provision(context.Background(), sampleSpec()); err == nil {
		t.Fatal("expected provision error")
	}
	var cr arenav1.Arena
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "arena-m1"}, &cr)
	if !apierrors.IsNotFound(err) {
		t.Fatal("failed arena CR was left behind (leak)")
	}
}

// Hazır olmazsa zaman aşımı + temizlik.
func TestK8sProvisionTimeoutCleansUp(t *testing.T) {
	p, c := k8sFixture(t)
	p.ReadyTimeout = 300 * time.Millisecond

	if _, err := p.Provision(context.Background(), sampleSpec()); err == nil {
		t.Fatal("expected timeout error")
	}
	var cr arenav1.Arena
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "arena-m1"}, &cr); !apierrors.IsNotFound(err) {
		t.Fatal("timed-out arena CR was left behind (leak)")
	}
}

// Destroy idempotenttir (telafi yolları tekrar çalışabilir).
func TestK8sDestroyIdempotent(t *testing.T) {
	p, _ := k8sFixture(t)
	if err := p.Destroy(context.Background(), "yok"); err != nil {
		t.Fatalf("destroying a missing arena must be a no-op: %v", err)
	}
}
