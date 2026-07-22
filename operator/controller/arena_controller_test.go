package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	arenav1 "shardlands/operator/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := arenav1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

type fixture struct {
	t   *testing.T
	c   client.Client
	r   *ArenaReconciler
	now time.Time
}

func newFixture(t *testing.T, objs ...client.Object) *fixture {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&arenav1.Arena{}).
		Build()
	f := &fixture{t: t, c: c, now: time.Now()}
	f.r = &ArenaReconciler{Client: c, Scheme: s, Now: func() time.Time { return f.now }}
	return f
}

func (f *fixture) reconcile(name string) ctrl.Result {
	f.t.Helper()
	res, err := f.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: name},
	})
	if err != nil {
		f.t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (f *fixture) arena(name string) *arenav1.Arena {
	f.t.Helper()
	var a arenav1.Arena
	if err := f.c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, &a); err != nil {
		f.t.Fatalf("get arena: %v", err)
	}
	return &a
}

func (f *fixture) pod(name string) (*corev1.Pod, bool) {
	f.t.Helper()
	var p corev1.Pod
	err := f.c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, &p)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		f.t.Fatalf("get pod: %v", err)
	}
	return &p, true
}

func newArena(name string) *arenav1.Arena {
	return &arenav1.Arena{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: arenav1.ArenaSpec{
			Mode: "1v1",
			Players: []arenav1.ArenaPlayer{
				{ID: "p1", Name: "bir", Team: 0},
				{ID: "p2", Name: "iki", Team: 1},
			},
			TTLSeconds: 120,
		},
	}
}

// İlk reconcile finalizer ekler; ikincisi Pod'u yaratır ve durumu
// Pending yapar. Pod, spec'ten türetilir (mod ve oyuncular env'de).
func TestReconcileCreatesPod(t *testing.T) {
	f := newFixture(t, newArena("a1"))

	f.reconcile("a1") // finalizer
	if got := f.arena("a1").Finalizers; len(got) != 1 || got[0] != finalizer {
		t.Fatalf("finalizers = %v", got)
	}

	f.reconcile("a1") // pod
	pod, ok := f.pod("a1-pod")
	if !ok {
		t.Fatal("pod not created")
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restart policy = %v, want Never", pod.Spec.RestartPolicy)
	}
	env := map[string]string{}
	for _, e := range pod.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["ARENA_MODE"] != "1v1" || env["ARENA_PLAYERS"] != "p1:0,p2:1" {
		t.Fatalf("env = %v", env)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != "a1" {
		t.Fatalf("owner refs = %v (GC için gerekli)", pod.OwnerReferences)
	}

	a := f.arena("a1")
	if a.Status.Phase != arenav1.PhasePending || a.Status.PodName != "a1-pod" {
		t.Fatalf("status = %+v", a.Status)
	}
	if a.Status.StartTime == nil {
		t.Fatal("start time not set")
	}
}

// İDEMPOTENTLİK: tekrar tekrar reconcile ikinci bir Pod yaratmaz.
func TestReconcileIsIdempotent(t *testing.T) {
	f := newFixture(t, newArena("a1"))
	f.reconcile("a1")
	f.reconcile("a1")

	for i := 0; i < 5; i++ {
		f.reconcile("a1")
	}
	var pods corev1.PodList
	if err := f.c.List(context.Background(), &pods, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want 1 (reconcile must converge, not accumulate)", len(pods.Items))
	}
}

// Pod Running olunca durum Running olur ve endpoint yayınlanır.
func TestReconcileReflectsPodRunning(t *testing.T) {
	f := newFixture(t, newArena("a1"))
	f.reconcile("a1")
	f.reconcile("a1")

	pod, _ := f.pod("a1-pod")
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.1.2.3"
	if err := f.c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}

	f.reconcile("a1")
	a := f.arena("a1")
	if a.Status.Phase != arenav1.PhaseRunning {
		t.Fatalf("phase = %s, want Running", a.Status.Phase)
	}
	if a.Status.Endpoint != "10.1.2.3:7777" {
		t.Fatalf("endpoint = %q", a.Status.Endpoint)
	}
}

// Pod bitince durum Completed olur ve Pod TEMİZLENİR.
func TestReconcileCompletesAndCleansUp(t *testing.T) {
	f := newFixture(t, newArena("a1"))
	f.reconcile("a1")
	f.reconcile("a1")

	pod, _ := f.pod("a1-pod")
	pod.Status.Phase = corev1.PodSucceeded
	if err := f.c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}

	f.reconcile("a1") // durum Completed
	if got := f.arena("a1").Status.Phase; got != arenav1.PhaseCompleted {
		t.Fatalf("phase = %s, want Completed", got)
	}
	f.reconcile("a1") // temizlik
	if _, ok := f.pod("a1-pod"); ok {
		t.Fatal("pod not cleaned up after completion")
	}
}

// Pod başarısızsa durum Failed olur.
func TestReconcileMarksFailed(t *testing.T) {
	f := newFixture(t, newArena("a1"))
	f.reconcile("a1")
	f.reconcile("a1")

	pod, _ := f.pod("a1-pod")
	pod.Status.Phase = corev1.PodFailed
	if err := f.c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	f.reconcile("a1")
	if got := f.arena("a1").Status.Phase; got != arenav1.PhaseFailed {
		t.Fatalf("phase = %s, want Failed", got)
	}
}

// TTL aşılırsa arena tamamlandı sayılır (kaçak instance emniyeti).
func TestReconcileTTLExpiry(t *testing.T) {
	f := newFixture(t, newArena("a1"))
	f.reconcile("a1")
	f.reconcile("a1") // pod + StartTime

	f.now = f.now.Add(3 * time.Minute) // TTL 120s
	f.reconcile("a1")

	a := f.arena("a1")
	if a.Status.Phase != arenav1.PhaseCompleted {
		t.Fatalf("phase = %s, want Completed (ttl)", a.Status.Phase)
	}
	if a.Status.Message != "ttl exceeded" {
		t.Fatalf("message = %q", a.Status.Message)
	}
	f.reconcile("a1")
	if _, ok := f.pod("a1-pod"); ok {
		t.Fatal("pod survived ttl cleanup")
	}
}

// Silme: finalizer Pod temizliğini garanti eder, sonra kaldırılır.
func TestReconcileFinalizerCleansUpOnDelete(t *testing.T) {
	f := newFixture(t, newArena("a1"))
	f.reconcile("a1")
	f.reconcile("a1")
	if _, ok := f.pod("a1-pod"); !ok {
		t.Fatal("pod missing")
	}

	a := f.arena("a1")
	if err := f.c.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	// Finalizer yüzünden kaynak hâlâ var (silme işaretli).
	f.reconcile("a1")

	if _, ok := f.pod("a1-pod"); ok {
		t.Fatal("pod not deleted during finalize")
	}
	var got arenav1.Arena
	err := f.c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "a1"}, &got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("arena still present after finalize: %v (finalizers=%v)", err, got.Finalizers)
	}
}

// Olmayan kaynak için reconcile hatasız geçer (silinmiş olabilir).
func TestReconcileMissingResource(t *testing.T) {
	f := newFixture(t)
	if res := f.reconcile("yok"); res.Requeue {
		t.Fatal("missing resource should not requeue")
	}
}
