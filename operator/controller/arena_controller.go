// Package controller, Arena CRD'sinin reconcile döngüsüdür.
//
// Reconcile'ın sözleşmesi: FONKSİYON İDEMPOTENT VE SEVİYE-TETİKLEMELİ
// (level-triggered) olmalıdır. "Şu olay oldu, şunu yap" değil; "arzu
// edilen durum bu, gerçek durum şu, farkı kapat" der. Aynı Arena için
// yüz kez çağrılsa da sonuç aynıdır. Kubernetes controller'larının
// dayanıklılığı buradan gelir: kaçan olay, yeniden başlatma veya
// sıralama bozukluğu düzeltilebilir — döngü tekrar bakar ve düzeltir.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	arenav1 "shardlands/operator/api/v1alpha1"
)

const (
	// defaultTTL, spec'te belirtilmezse arenanın azami ömrü.
	defaultTTL = 5 * time.Minute
	// defaultImage, arena sunucusu imajı.
	defaultImage = "shardlands/arena:dev"
	// retainAfterEnd, biten arena kaydının silinmeden önce görünür
	// kalacağı süre (hata ayıklama penceresi).
	retainAfterEnd = 2 * time.Minute
	// finalizer, temizlik garantisi için (Pod'u biz sildik mi?).
	finalizer = "shardlands.dev/arena-cleanup"
)

// ArenaReconciler, Arena kaynaklarını uzlaştırır.
type ArenaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Now, saat kaynağı (TTL testleri için enjekte edilebilir).
	Now func() time.Time
}

func (r *ArenaReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile, tek bir Arena için arzu-gerçek farkını kapatır.
func (r *ArenaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var a arenav1.Arena
	if err := r.Get(ctx, req.NamespacedName, &a); err != nil {
		// Silinmiş: yapacak bir şey yok (Pod'u ownerRef zaten toplar).
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Silinme işareti varsa temizliği yap ve finalizer'ı kaldır.
	if !a.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &a)
	}
	if !containsString(a.Finalizers, finalizer) {
		a.Finalizers = append(a.Finalizers, finalizer)
		if err := r.Update(ctx, &a); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal aşama: Pod'u temizle, sonra KAYDI DA topla.
	//
	// Kaydı süresiz bırakmak zararsız görünüyordu; değildi. Bitmiş bir
	// Arena kaydı, aynı adı yeniden üreten bir sunucu için görünmez bir
	// mayındır: Create "zaten var" der, sağlayıcı bunu saga yeniden
	// denemesi sanar ve Running bekler — ama kayıt Completed olduğu için
	// o an hiç gelmez. Chaos deneyi 5'te tam olarak bu yaşandı.
	//
	// retainAfterEnd kadar bekliyoruz ki `kubectl get arenas` çıktısında
	// biten maç bir süre görünsün (hata ayıklama), sonra siliniyor.
	if a.Status.Phase == arenav1.PhaseCompleted || a.Status.Phase == arenav1.PhaseFailed {
		if err := r.deletePod(ctx, &a); err != nil {
			return ctrl.Result{}, err
		}
		if a.Status.EndTime == nil {
			now := metav1.NewTime(r.now())
			a.Status.EndTime = &now
			return ctrl.Result{Requeue: true}, r.Status().Update(ctx, &a)
		}
		if kalan := retainAfterEnd - r.now().Sub(a.Status.EndTime.Time); kalan > 0 {
			return ctrl.Result{RequeueAfter: kalan}, nil
		}
		lg.Info("biten arena kaydı toplanıyor", "arena", a.Name)
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, &a))
	}

	// TTL: arena fazla yaşadıysa tamamlandı say (kaçak instance emniyeti).
	if a.Status.StartTime != nil && r.now().Sub(a.Status.StartTime.Time) > r.ttl(&a) {
		lg.Info("arena ttl exceeded", "arena", a.Name)
		a.Status.Phase = arenav1.PhaseCompleted
		a.Status.Message = "ttl exceeded"
		if err := r.Status().Update(ctx, &a); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Arzu edilen Pod var mı?
	podName := podNameFor(&a)
	var pod corev1.Pod
	err := r.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: podName}, &pod)
	switch {
	case apierrors.IsNotFound(err):
		// YOK → yarat (arzu edilen duruma yaklaş).
		desired := r.desiredPod(&a, podName)
		if err := ctrl.SetControllerReference(&a, desired, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, err
		}
		now := metav1.NewTime(r.now())
		a.Status.Phase = arenav1.PhasePending
		a.Status.PodName = podName
		a.Status.StartTime = &now
		if err := r.Status().Update(ctx, &a); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	case err != nil:
		return ctrl.Result{}, err
	}

	// VAR → durumu yansıt.
	next := a.Status.Phase
	switch pod.Status.Phase {
	case corev1.PodRunning:
		next = arenav1.PhaseRunning
	case corev1.PodSucceeded:
		next = arenav1.PhaseCompleted
	case corev1.PodFailed:
		next = arenav1.PhaseFailed
	default:
		next = arenav1.PhasePending
	}
	endpoint := ""
	if pod.Status.PodIP != "" {
		endpoint = fmt.Sprintf("%s:%d", pod.Status.PodIP, 7777)
	}
	if next != a.Status.Phase || endpoint != a.Status.Endpoint || a.Status.PodName != podName {
		a.Status.Phase, a.Status.Endpoint, a.Status.PodName = next, endpoint, podName
		if err := r.Status().Update(ctx, &a); err != nil {
			return ctrl.Result{}, err
		}
	}
	// TTL'e kadar periyodik kontrol (seviye-tetiklemeli döngü).
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// finalize, kaynak silinirken Pod'u temizler ve finalizer'ı kaldırır.
func (r *ArenaReconciler) finalize(ctx context.Context, a *arenav1.Arena) (ctrl.Result, error) {
	if err := r.deletePod(ctx, a); err != nil {
		return ctrl.Result{}, err
	}
	a.Finalizers = removeString(a.Finalizers, finalizer)
	return ctrl.Result{}, r.Update(ctx, a)
}

func (r *ArenaReconciler) deletePod(ctx context.Context, a *arenav1.Arena) error {
	var pod corev1.Pod
	err := r.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: podNameFor(a)}, &pod)
	if apierrors.IsNotFound(err) {
		return nil // idempotent
	}
	if err != nil {
		return err
	}
	if err := r.Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *ArenaReconciler) ttl(a *arenav1.Arena) time.Duration {
	if a.Spec.TTLSeconds > 0 {
		return time.Duration(a.Spec.TTLSeconds) * time.Second
	}
	return defaultTTL
}

func podNameFor(a *arenav1.Arena) string { return a.Name + "-pod" }

// desiredPod, spec'ten türetilen Pod (arzu edilen durum).
func (r *ArenaReconciler) desiredPod(a *arenav1.Arena, name string) *corev1.Pod {
	image := a.Spec.Image
	if image == "" {
		image = defaultImage
	}
	players := ""
	for i, p := range a.Spec.Players {
		if i > 0 {
			players += ","
		}
		players += fmt.Sprintf("%s:%d", p.ID, p.Team)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: a.Namespace,
			Labels: map[string]string{
				"app":              "shardlands-arena",
				"shardlands/arena": a.Name,
			},
			// Mesh annotation'ları Pod'a BURADA yazılır, çünkü bu Pod'u
			// bir manifest değil biz üretiyoruz. Native sidecar şart:
			// klasik sidecar hiç çıkmaz, maç bitse de Pod Running kalır
			// ve aşağıdaki Succeeded akışı hiç tetiklenmezdi.
			Annotations: map[string]string{
				"linkerd.io/inject": "enabled",
				"config.alpha.linkerd.io/proxy-enable-native-sidecar": "true",
				// Prometheus keşfi: arena Pod'ları da çekilsin. Kısa
				// ömürlü oldukları için tam örnekleme garanti değil
				// (docs/observability.md §5) — yine de tick süresi ve
				// Go çalışma zamanı sinyalleri buradan gelir.
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "7778",
				"prometheus.io/path":   "/metrics",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, // maç tek seferlik
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptr(true),
				// Sayısal olmalı: kubelet, imajdaki isimli kullanıcının
				// (`USER nonroot`) root olmadığını doğrulayamaz.
				RunAsUser: ptr(int64(65532)),
			},
			Containers: []corev1.Container{{
				Name:  "arena",
				Image: image,
				Env: []corev1.EnvVar{
					{Name: "ARENA_ID", Value: a.Name},
					{Name: "ARENA_MODE", Value: a.Spec.Mode},
					{Name: "ARENA_PLAYERS", Value: players},
				},
				Ports: []corev1.ContainerPort{{ContainerPort: 7777, Name: "game"}},
				// Arena durumsuz ve kısa ömürlü: diske yazmaz, ayrıcalık
				// istemez. En dar profil (zero trust: iş yükü tarafı).
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr(false),
					ReadOnlyRootFilesystem:   ptr(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
}

// SetupWithManager, controller'ı manager'a bağlar.
func (r *ArenaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&arenav1.Arena{}).
		Owns(&corev1.Pod{}). // Pod değişimleri de reconcile tetikler
		Complete(r)
}

// ptr, Kubernetes API'sinin "belirtilmedi" ile "false" ayrımını
// yapabilmesi için gereken işaretçileri üretir.
func ptr[T any](v T) *T { return &v }

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func removeString(xs []string, s string) []string {
	out := xs[:0]
	for _, x := range xs {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}
