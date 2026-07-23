// Package v1alpha1, Shardlands'in Arena özel kaynağını (CRD) tanımlar.
//
// Operator pattern: Kubernetes'in kendi denetleyici mantığını genişletir.
// Özel bir kaynak tipi tanımlarsın (Arena), bir RECONCILE DÖNGÜSÜ
// sürekli "arzu edilen durum" (spec) ile "gerçek durum"u (cluster'daki
// Pod) karşılaştırıp farkı kapatır.
//
// Arena neden bu modele uyuyor? Talep üzerine doğan, kısa ömürlü, TTL'i
// olan ve bitince temizlenmesi gereken bir iş yükü. Bunu elle yönetmek
// (yarat/izle/sil) tam olarak controller'ın işidir; imperatif kod yerine
// DEKLARATİF bir kayıt bırakırız ve küme onu gerçek kılar. Kazanç:
// çökme/yeniden başlatma dayanıklılığı (durum etcd'de, döngü kaldığı
// yerden devam eder) ve kubectl ile gözlemlenebilirlik.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion, CRD'nin grup/sürümü.
var (
	GroupVersion  = schema.GroupVersion{Group: "shardlands.dev", Version: "v1alpha1"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &Arena{}, &ArenaList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// ArenaPlayer, maça atanmış oyuncu.
type ArenaPlayer struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
	Team int    `json:"team"`
}

// ArenaSpec, ARZU EDİLEN durum: hangi modda, kimlerle, ne kadar süre.
type ArenaSpec struct {
	// Mode: "1v1" veya "2v2".
	Mode string `json:"mode"`
	// Players, maça atanacak oyuncular.
	Players []ArenaPlayer `json:"players"`
	// TTLSeconds, arena bu süreden uzun yaşarsa temizlenir (kaçak
	// instance'lara karşı emniyet supabı). 0 ise varsayılan uygulanır.
	TTLSeconds int32 `json:"ttlSeconds,omitempty"`
	// Image, arena sunucusunun konteyner imajı.
	Image string `json:"image,omitempty"`
}

// ArenaPhase, yaşam döngüsü aşaması.
type ArenaPhase string

const (
	PhasePending   ArenaPhase = "Pending"   // Pod henüz yok/hazır değil
	PhaseRunning   ArenaPhase = "Running"   // maç sürüyor
	PhaseCompleted ArenaPhase = "Completed" // maç bitti, temizlenecek
	PhaseFailed    ArenaPhase = "Failed"    // Pod başarısız
)

// ArenaStatus, GERÇEK durum (controller yazar).
type ArenaStatus struct {
	Phase     ArenaPhase   `json:"phase,omitempty"`
	PodName   string       `json:"podName,omitempty"`
	Endpoint  string       `json:"endpoint,omitempty"`
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// EndTime, terminal faza geçiş anı. Kaydın ne zaman toplanacağını
	// buradan hesaplıyoruz: biten maç bir süre görünür kalsın (hata
	// ayıklama), sonra silinsin. Süresiz bırakmak kimlik çakışmasına
	// yol açıyordu (bkz. controller, chaos deneyi 5).
	EndTime *metav1.Time `json:"endTime,omitempty"`
	Message string       `json:"message,omitempty"`
}

// Arena, özel kaynak.
type Arena struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ArenaSpec   `json:"spec,omitempty"`
	Status ArenaStatus `json:"status,omitempty"`
}

// ArenaList, koleksiyon.
type ArenaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Arena `json:"items"`
}

// ---- DeepCopy implementasyonları ----
//
// Normalde controller-gen üretir; burada elle yazıldı ki proje ek bir
// kod üretim aracına bağımlı olmasın (Faz 0'ın "sıfırdan yaz" çizgisiyle
// tutarlı ve CRD tiplerinde bu kadarı okunur).

func (in *ArenaPlayer) DeepCopyInto(out *ArenaPlayer) { *out = *in }

func (in *ArenaSpec) DeepCopyInto(out *ArenaSpec) {
	*out = *in
	if in.Players != nil {
		out.Players = make([]ArenaPlayer, len(in.Players))
		copy(out.Players, in.Players)
	}
}

func (in *ArenaStatus) DeepCopyInto(out *ArenaStatus) {
	*out = *in
	if in.StartTime != nil {
		t := *in.StartTime
		out.StartTime = &t
	}
	if in.EndTime != nil {
		t := *in.EndTime
		out.EndTime = &t
	}
}

func (in *Arena) DeepCopyInto(out *Arena) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *Arena) DeepCopy() *Arena {
	if in == nil {
		return nil
	}
	out := new(Arena)
	in.DeepCopyInto(out)
	return out
}

func (in *Arena) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *ArenaList) DeepCopyInto(out *ArenaList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Arena, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *ArenaList) DeepCopy() *ArenaList {
	if in == nil {
		return nil
	}
	out := new(ArenaList)
	in.DeepCopyInto(out)
	return out
}

func (in *ArenaList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
